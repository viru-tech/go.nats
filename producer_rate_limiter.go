package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

var errBackpressureControllerNotFound = errors.New("backpressure controller not found")

// BackpressureConfig maps stream names to their backpressure configuration.
type BackpressureConfig map[string]BackpressureStreamConfig

// ThrottlePolicyInput contains the current stream fill and previous limiter state.
type ThrottlePolicyInput struct {
	Now                 time.Time
	PreviousUpdatedAt   time.Time
	StreamName          string
	BytesFillRatio      float64
	MsgsFillRatio       float64
	ThresholdBytesRatio float64
	ThresholdMsgsRatio  float64
	MaxDelay            time.Duration
	PreviousDelay       time.Duration
	PreviousFactor      float64
}

// ThrottlePolicyResult is the throttle factor chosen by a custom policy.
type ThrottlePolicyResult struct {
	// Factor is clamped to [0, 1] before converting it to a delay.
	Factor float64
	// IsMaxDelay forces the controller into the max-delay state regardless of Factor.
	IsMaxDelay bool
}

// ThrottlePolicyFunc allows callers to override the built-in throttle curve.
type ThrottlePolicyFunc func(input ThrottlePolicyInput) ThrottlePolicyResult

// BackpressureStreamConfig controls proportional throttling for a single stream.
// Once either fill percentage crosses its threshold, a per-publish delay is injected.
// By default the delay follows an ease-in-out curve from 0 at the threshold
// to MaxDelay at 100% fill, then stays clamped at MaxDelay beyond that point.
//
// This design is intentionally uncoordinated: each producer instance independently
// observes the same stream state and applies the same delay, so N producers with the
// same config all throttle simultaneously without needing to know about each other.
type BackpressureStreamConfig struct {
	// ThrottlePolicy overrides the built-in curve when set.
	ThrottlePolicy ThrottlePolicyFunc
	// OnThrottleStateChange is called once when fill crosses the threshold and throttling begins.
	// It is edge-triggered: called on the transition from no delay to any delay.
	OnThrottleStateChange func(isThrottled bool)
	// OnMaxDelayStateChange is called once when max delay is reached.
	// It is edge-triggered: called on the transition into the fully-saturated state.
	OnMaxDelayStateChange func(isFull bool)
	// MaxDelay is the per-publish delay applied once fill reaches 100%.
	MaxDelay time.Duration
	// MaxMsgs is the configured message capacity used to compute message-fill ratio.
	// Zero disables message-based throttling.
	MaxMsgs uint64
	// MaxBytes is the configured byte capacity used to compute byte-fill ratio.
	// Zero disables byte-based throttling.
	MaxBytes uint64
	// ThresholdMsgsPercent is the message-fill percentage (0–100) at which throttling begins.
	// Zero disables message-based throttling.
	ThresholdMsgsPercent uint8
	// ThresholdBytesPercent is the byte-fill percentage (0–100) at which throttling begins.
	// Zero disables byte-based throttling.
	ThresholdBytesPercent uint8
}

type backpressureController struct {
	updatedAt     time.Time
	config        BackpressureStreamConfig
	delay         time.Duration
	factor        float64
	mu            sync.RWMutex
	wasThrottling bool
	wasStreamFull bool
}

func (bc *backpressureController) getDelay() time.Duration {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.delay
}

func (bc *backpressureController) snapshot() (time.Duration, float64, time.Time) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.delay, bc.factor, bc.updatedAt
}

func (bc *backpressureController) setState(d time.Duration, factor float64, updatedAt time.Time) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.delay = d
	bc.factor = factor
	bc.updatedAt = updatedAt
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

		ticker := time.NewTicker(2 * time.Second)
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
		delay, factor, isThrottling, isStreamFull, err := p.computeDelay(ctx, streamName, ctrl)
		if err != nil {
			p.logger.Error("failed to compute backpressure delay",
				zap.String("stream", streamName),
				zap.Error(err),
			)
			continue
		}
		ctrl.setState(delay, factor, time.Now())

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
	ctx context.Context, streamName string, ctrl *backpressureController,
) (time.Duration, float64, bool, bool, error) {
	stream, err := p.js.Stream(ctx, streamName)
	if err != nil {
		return 0, 0, false, false, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, 0, false, false, err
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo(streamName, info, ctrl)

	return delay, factor, isThrottling, isStreamFull, nil
}

func computeThrottleStateFromStreamInfo(
	streamName string, info *jetstream.StreamInfo, ctrl *backpressureController,
) (time.Duration, float64, bool, bool) {
	cfg := ctrl.config

	var bytesFillRatio float64
	if cfg.MaxBytes > 0 {
		bytesFillRatio = normalize(float64(info.State.Bytes) / float64(cfg.MaxBytes))
	}

	var msgsFillRatio float64
	if cfg.MaxMsgs > 0 {
		msgsFillRatio = normalize(float64(info.State.Msgs) / float64(cfg.MaxMsgs))
	}

	previousDelay, previousFactor, previousUpdatedAt := ctrl.snapshot()
	input := ThrottlePolicyInput{
		StreamName:          streamName,
		Now:                 time.Now(),
		BytesFillRatio:      bytesFillRatio,
		MsgsFillRatio:       msgsFillRatio,
		ThresholdBytesRatio: float64(cfg.ThresholdBytesPercent) / 100.0,
		ThresholdMsgsRatio:  float64(cfg.ThresholdMsgsPercent) / 100.0,
		MaxDelay:            cfg.MaxDelay,
		PreviousDelay:       previousDelay,
		PreviousFactor:      previousFactor,
		PreviousUpdatedAt:   previousUpdatedAt,
	}

	var result ThrottlePolicyResult
	if cfg.ThrottlePolicy != nil {
		result = cfg.ThrottlePolicy(input)
	} else {
		result = ThrottlePolicyResult{
			Factor: builtInThrottleFactor(&input),
		}
	}

	factor := normalize(result.Factor)
	if result.IsMaxDelay {
		factor = 1
	}

	delay := time.Duration(float64(cfg.MaxDelay) * factor)
	isThrottling := factor > 0
	isStreamFull := result.IsMaxDelay || factor >= 1

	return delay, factor, isThrottling, isStreamFull
}

func builtInThrottleFactor(input *ThrottlePolicyInput) float64 {
	bytesFactor := throttleFactor(input.BytesFillRatio, input.ThresholdBytesRatio)
	msgsFactor := throttleFactor(input.MsgsFillRatio, input.ThresholdMsgsRatio)
	if msgsFactor > bytesFactor {
		return msgsFactor
	}

	return bytesFactor
}

// throttleFactor returns a value in [0, 1] representing how much to throttle.
func throttleFactor(fill, threshold float64) float64 {
	if fill <= threshold || threshold >= 1 {
		return 0
	}
	if fill >= 1 {
		return 1
	}
	t := (fill - threshold) / (1 - threshold) // normalized to [0, 1]

	return t * t * t * t
}

func normalize(v float64) float64 {
	return min(1, max(0, v))
}
