package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	defaultMaxDelayAtPercent = 0.80
)

var errBackpressureControllerNotFound = errors.New("backpressure controller not found")

// BackpressureConfig maps stream names to their backpressure configuration.
type BackpressureConfig map[string]BackpressureStreamConfig

// BackpressureStreamConfig controls proportional throttling for a single stream.
// Once either fill percentage crosses its threshold, a per-publish delay is injected.
// The delay grows quadratically from 0 at the threshold to MaxDelay at MaxDelayAtPercent,
// then stays clamped at MaxDelay beyond that point.
//
// This design is intentionally uncoordinated: each producer instance independently
// observes the same stream state and applies the same delay, so N producers with the
// same config all throttle simultaneously without needing to know about each other.
type BackpressureStreamConfig struct {
	// OnThrottleStateChange is called once when fill crosses the threshold and throttling begins.
	// It is edge-triggered: called on the transition from no delay to any delay.
	OnThrottleStateChange func(isThrottled bool)
	// OnMaxDelayStateChange is called once when max delay is reached.
	// It is edge-triggered: called on the transition into the fully-saturated state.
	OnMaxDelayStateChange func(isFull bool)
	// MaxDelay is the per-publish delay applied once fill reaches MaxDelayAtPercent.
	MaxDelay time.Duration
	// MaxDelayAtPercent is the fill percentage (0–100) at which MaxDelay is fully applied.
	// Defaults to 80 when zero. Fill above this level keeps the delay clamped at MaxDelay.
	MaxDelayAtPercent uint8
	// ThresholdMsgsPercent is the message-fill percentage (0–100) at which throttling begins.
	// Zero disables message-based throttling.
	ThresholdMsgsPercent uint8
	// ThresholdBytesPercent is the byte-fill percentage (0–100) at which throttling begins.
	// Zero disables byte-based throttling.
	ThresholdBytesPercent uint8
}

type backpressureController struct {
	config        BackpressureStreamConfig
	mu            sync.RWMutex
	delay         time.Duration
	wasThrottling bool
	wasStreamFull bool
}

func (bc *backpressureController) getDelay() time.Duration {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.delay
}

func (bc *backpressureController) setDelay(d time.Duration) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.delay = d
}

func (p *Producer) resolveBackpressureController(ctx context.Context, subject string) (*backpressureController, error) {
	if len(p.backPressure) == 0 {
		return nil, errBackpressureControllerNotFound
	}
	if cachedStream, ok := p.streamBySubject.Load(subject); ok {
		streamName, _ := cachedStream.(string)
		ctrl, ok := p.backPressure[streamName]
		if !ok {
			return nil, errBackpressureControllerNotFound
		}
		return ctrl, nil
	}

	streamName, err := p.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, errBackpressureControllerNotFound
		}

		return nil, err
	}
	p.streamBySubject.Store(subject, streamName)

	ctrl, ok := p.backPressure[streamName]
	if !ok {
		return nil, errBackpressureControllerNotFound
	}

	return ctrl, nil
}

// throttle blocks for the computed backpressure delay for the given stream.
// Returns ctx.Err() if the context is canceled while waiting.
func (p *Producer) throttle(ctx context.Context, subject string) error {
	ctrl, err := p.resolveBackpressureController(ctx, subject)
	if err != nil {
		if errors.Is(err, errBackpressureControllerNotFound) {
			return nil
		}
		p.logger.Error("failed to resolve stream for backpressure",
			zap.String("subject", subject),
			zap.Error(err),
		)
		return nil
	}
	delay := ctrl.getDelay()
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// applyBackpressure starts a background goroutine that periodically polls each
// configured stream, updates the per-stream throttle delay, and fires edge-triggered
// callbacks when throttling starts or the stream reaches 100% fill.
func (p *Producer) applyBackpressure(ctx context.Context) {
	p.backPressureWG.Go(func() {
		p.calculateBackpressureDelay(ctx)

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.calculateBackpressureDelay(ctx)
			}
		}
	})
}

func (p *Producer) calculateBackpressureDelay(ctx context.Context) {
	for streamName, ctrl := range p.backPressure {
		delay, isThrottling, isStreamFull, err := p.computeDelay(ctx, streamName, ctrl.config)
		if err != nil {
			p.logger.Error("failed to compute backpressure delay",
				zap.String("stream", streamName),
				zap.Error(err),
			)
			continue
		}
		ctrl.setDelay(delay)

		if isThrottling != ctrl.wasThrottling && ctrl.config.OnThrottleStateChange != nil {
			ctrl.config.OnThrottleStateChange(isThrottling)
		}
		if isStreamFull != ctrl.wasStreamFull && ctrl.config.OnMaxDelayStateChange != nil {
			ctrl.config.OnMaxDelayStateChange(isStreamFull)
		}
		ctrl.wasThrottling = isThrottling
		ctrl.wasStreamFull = isStreamFull
	}
}

// computeDelay fetches stream state and returns the delay to inject before the next
// publish, whether throttling is active, and whether the stream is at 100% fill.
func (p *Producer) computeDelay(
	ctx context.Context, streamName string, cfg BackpressureStreamConfig,
) (time.Duration, bool, bool, error) {
	stream, err := p.js.Stream(ctx, streamName)
	if err != nil {
		return 0, false, false, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, false, false, err
	}

	delay, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo(info, cfg)

	return delay, isThrottling, isStreamFull, nil
}

func computeThrottleStateFromStreamInfo(
	info *jetstream.StreamInfo, cfg BackpressureStreamConfig,
) (time.Duration, bool, bool) {
	maxDelayAt := float64(cfg.MaxDelayAtPercent) / 100.0
	if maxDelayAt == 0 {
		maxDelayAt = defaultMaxDelayAtPercent
	}

	var (
		factor       float64
		isStreamFull bool
	)

	if cfg.ThresholdBytesPercent > 0 && info.Config.MaxBytes > 0 {
		fill := float64(info.State.Bytes) / float64(info.Config.MaxBytes)
		if fill >= maxDelayAt {
			return cfg.MaxDelay, true, true
		}

		factor, isStreamFull = throttleFactor(fill, float64(cfg.ThresholdBytesPercent)/100.0, maxDelayAt)
	}

	if !isStreamFull && cfg.ThresholdMsgsPercent > 0 && info.Config.MaxMsgs > 0 {
		fill := float64(info.State.Msgs) / float64(info.Config.MaxMsgs)
		if fill >= maxDelayAt {
			return cfg.MaxDelay, true, true
		}
		f, isFull := throttleFactor(fill, float64(cfg.ThresholdMsgsPercent)/100.0, maxDelayAt)
		if f > factor {
			factor = f
		}

		if isFull {
			isStreamFull = true
		}
	}

	isThrottling := factor > 0
	delay := time.Duration(float64(cfg.MaxDelay) * factor)

	return delay, isThrottling, isStreamFull
}

// throttleFactor returns a value in [0, 1] representing how much to throttle and returns (1, true)
// if stream has reached maxDelayAt.
// Returns 0 at or below threshold, grows quadratically to 1 at maxDelayAt,
// and stays clamped at 1 beyond that.
// The quadratic curve stays gentle near the threshold and steepens as fill
// approaches maxDelayAt, giving producers an early warning before hitting MaxDelay.
func throttleFactor(fill, threshold, maxDelayAt float64) (float64, bool) {
	if fill <= threshold || threshold >= maxDelayAt {
		return 0, false
	}
	if fill >= maxDelayAt {
		return 1.0, true
	}
	t := (fill - threshold) / (maxDelayAt - threshold) // normalised to [0, 1]

	return t * t, false
}
