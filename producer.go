package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/viru-tech/fastime/v2"
	"go.uber.org/zap"
)

//go:generate mockgen -source=producer.go -package=nats -destination=producer_mock.go
//go:generate mockgen -source=./vendor/github.com/nats-io/nats.go/jetstream/jetstream.go -imports=jetstream=github.com/nats-io/nats.go/jetstream -package=nats -mock_names=JetStream=MockJetStream -destination=jetstream_mock_test.go
//go:generate mockgen -source=./vendor/github.com/nats-io/nats.go/jetstream/publish.go -imports=publish=github.com/nats-io/nats.go/publish -package=nats -mock_names=JetStream=MockPublish -destination=publish_mock_test.go

// ProducerNats is an interface defining the contract for NATS message producers.
type ProducerNats interface {
	// Closer closes the NATS connection.
	io.Closer
	// ProduceJSON publishes a JSON message to the specified subject.
	ProduceJSON(ctx context.Context, subject string, v any) error
	// ProduceBytes publishes a []bytes to the specified subject.
	ProduceBytes(ctx context.Context, subject string, data []byte) error
	// ProduceJSONAsync publishes async a JSON message to the specified subject.
	ProduceJSONAsync(subject string, v any) error
}

// ErrorHandler defines the error function that will be called when error is received.
type ErrorHandler func(err error, subject string, data []byte)

// AsyncPublishOutcome classifies terminal async publish results.
type AsyncPublishOutcome string

const (
	// AsyncPublishOutcomeAcked indicates the server confirmed the publish.
	AsyncPublishOutcomeAcked AsyncPublishOutcome = "acked"
	// AsyncPublishOutcomeFailed indicates the publish is known to have failed.
	AsyncPublishOutcomeFailed AsyncPublishOutcome = "failed"
	// AsyncPublishOutcomeUnknown indicates the payload may have reached the server,
	// but the client cannot determine the final state safely.
	AsyncPublishOutcomeUnknown AsyncPublishOutcome = "unknown"
)

var errProducerClosed = errors.New("producer is closed")

// AsyncPublishResult describes one terminal async publish result.
type AsyncPublishResult struct {
	Outcome AsyncPublishOutcome
	Err     error
	Subject string
	Data    []byte
}

// AsyncOutcomeHandler receives structured async publish results.
type AsyncOutcomeHandler func(result AsyncPublishResult)

// Producer is a NATS Producer that publishes messages to subjects.
type Producer struct {
	logger          *zap.Logger
	nc              *nats.Conn
	backPressure    map[string]*backpressureController
	js              jetstream.JetStream
	bpCancel        context.CancelFunc
	errorHandler    ErrorHandler
	asyncHandler    AsyncOutcomeHandler
	fallback        Fallback
	fallbackCh      chan fallbackRequest
	fastime         fastime.Fastime
	streamBySubject sync.Map
	closeOnce       sync.Once
	closed          atomic.Bool
	closedCh        chan struct{}

	fallbackTimeout time.Duration
	publishTimeout  time.Duration
	asyncAckTimeout time.Duration
	asyncMaxPending int

	asyncWG        sync.WaitGroup
	fallbackWG     sync.WaitGroup
	backPressureWG sync.WaitGroup

	fallbackBufferSize  int
	fallbackConcurrency int
	compression         bool
}

type fallbackRequest struct {
	subject string
	data    []byte
}

// NewProducer creates a new NATS Producer with the given URLs.
func NewProducer(urls []string, opts ...ProducerOption) (*Producer, error) {
	var err error
	p := &Producer{
		logger:              zap.NewNop(),
		fallbackTimeout:     time.Millisecond * 500,
		publishTimeout:      5 * time.Second,
		asyncAckTimeout:     5 * time.Second,
		asyncMaxPending:     10,
		fallbackBufferSize:  10,
		fallbackConcurrency: 1,
		fastime:             fastime.New().StartTimerD(context.Background(), time.Millisecond*5),
	}

	for _, opt := range opts {
		opt(p)
	}

	natsOpts := nats.Options{
		Servers:          urls,
		AllowReconnect:   true,
		Compression:      p.compression,
		MaxReconnect:     -1,
		ReconnectBufSize: -1,
		DisconnectedErrCB: func(_ *nats.Conn, err error) {
			if err != nil {
				p.logger.Error("connection to NATS lost, reconnecting...", zap.Error(err))
			}
		},
		ReconnectedCB: func(_ *nats.Conn) {
			p.logger.Info("connection to NATS restored")
		},
	}

	p.nc, err = natsOpts.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	jsOpts := make([]jetstream.JetStreamOpt, 0, 2)
	if p.asyncAckTimeout > 0 {
		jsOpts = append(jsOpts, jetstream.WithPublishAsyncTimeout(p.asyncAckTimeout))
	}
	if p.asyncMaxPending > 0 {
		jsOpts = append(jsOpts, jetstream.WithPublishAsyncMaxPending(p.asyncMaxPending))
	}

	p.js, err = jetstream.New(p.nc, jsOpts...)
	if err != nil {
		p.nc.Close()
		return nil, fmt.Errorf("failed to create JetStream instance: %w", err)
	}

	p.fallbackCh = make(chan fallbackRequest, p.fallbackBufferSize)
	p.closedCh = make(chan struct{})

	if len(p.backPressure) > 0 {
		backpressureCtx, cancel := context.WithCancel(context.Background())
		p.bpCancel = cancel
		p.applyBackpressure(backpressureCtx)
	}

	p.runFallbackWorker()

	return p, nil
}

// Close closes the NATS connection.
func (p *Producer) Close() error { //nolint:unparam
	p.closed.Store(true)
	p.closeOnce.Do(func() {
		if p.closedCh != nil {
			close(p.closedCh)
		}
	})
	if p.bpCancel != nil {
		p.bpCancel()
		p.backPressureWG.Wait()
	}
	p.js.CleanupPublisher()
	if p.nc != nil {
		p.nc.Close()
	}
	p.asyncWG.Wait()
	close(p.fallbackCh)
	p.fallbackWG.Wait()
	return nil
}

// ProduceJSON publishes a JSON message to the specified subject.
func (p *Producer) ProduceJSON(ctx context.Context, subject string, v any) error {
	data, err := jsoniter.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	return p.ProduceBytes(ctx, subject, data)
}

// ProduceJSONAsync publishes a JSON message to the specified subject asynchronously.
func (p *Producer) ProduceJSONAsync(subject string, v any) error {
	var err error

	data, err := jsoniter.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	if err := p.throttle(context.Background(), subject); err != nil {
		return err
	}

	if p.closed.Load() {
		p.logger.Warn("async publish rejected because producer is closing",
			zap.String("subject", subject),
		)
		return errProducerClosed
	}

	start := p.fastime.Now()
	pubAckFuture, err := p.js.PublishAsync(subject, data)
	if err != nil {
		incProducerSentCounter(subject, true)
		observeProducerAckWaitingTime(subject, true, time.Since(start))

		p.handleAsyncFailure(AsyncPublishResult{
			Outcome: AsyncPublishOutcomeFailed,
			Err:     err,
			Subject: subject,
			Data:    data,
		}, start)
		return err
	}

	p.trackAsyncPublish(pubAckFuture, start)
	return nil
}

// ProduceBytes publishes a []byte to the specified subject synchronously.
func (p *Producer) ProduceBytes(ctx context.Context, subject string, data []byte) error {
	if err := p.throttle(ctx, subject); err != nil {
		return err
	}

	pubCtx := ctx
	if p.publishTimeout > 0 {
		var pubCancel context.CancelFunc
		pubCtx, pubCancel = context.WithTimeout(ctx, p.publishTimeout)
		defer pubCancel()
	}

	start := p.fastime.Now()
	if _, err := p.js.Publish(pubCtx, subject, data); err != nil {
		incProducerSentCounter(subject, true)
		observeProducerAckWaitingTime(subject, true, time.Since(start))

		if p.fallback != nil {
			fbCtx, fbCancel := context.WithTimeout(ctx, p.fallbackTimeout)
			defer fbCancel()

			if err := p.fallback.SaveMessage(fbCtx, subject, data); err != nil {
				p.logger.Error("failed to save to fallback",
					zap.String("subject", subject),
					zap.Error(err),
				)
			}
		}
		return err
	}

	incProducerSentCounter(subject, false)
	observeProducerAckWaitingTime(subject, false, time.Since(start))
	return nil
}

// SaveMessage allows using Producer p as a Fallback. It simply
// pushes msg to the configured brokers. No error is ever returned,
// fallback should be configured for p to handle async Producer errors.
func (p *Producer) SaveMessage(ctx context.Context, subject string, msg []byte) error {
	return p.ProduceBytes(ctx, subject, msg)
}

func (p *Producer) trackAsyncPublish(pubAckFuture jetstream.PubAckFuture, sentTime time.Time) {
	subject := pubAckFuture.Msg().Subject
	incAsyncPublishPending(subject, 1)
	p.asyncWG.Go(func() {
		defer incAsyncPublishPending(subject, -1)

		select {
		case <-pubAckFuture.Ok():
			incProducerSentCounter(subject, false)
			observeProducerAckWaitingTime(subject, false, time.Since(sentTime))
			incAsyncPublishOutcomeCounter(subject, AsyncPublishOutcomeAcked)
			observeAsyncPublishResolutionTime(subject, AsyncPublishOutcomeAcked, time.Since(sentTime))
			p.handleAsyncResult(AsyncPublishResult{
				Outcome: AsyncPublishOutcomeAcked,
				Subject: subject,
				Data:    pubAckFuture.Msg().Data,
			})

		case err := <-pubAckFuture.Err():
			outcome := classifyAsyncPublishOutcome(err)
			result := AsyncPublishResult{
				Outcome: outcome,
				Err:     err,
				Subject: subject,
				Data:    pubAckFuture.Msg().Data,
			}
			p.handleAsyncFailure(result, sentTime)
		}
	})
}

func classifyAsyncPublishOutcome(err error) AsyncPublishOutcome {
	switch {
	case err == nil:
		return AsyncPublishOutcomeAcked
	case errors.Is(err, jetstream.ErrAsyncPublishTimeout):
		return AsyncPublishOutcomeUnknown
	case errors.Is(err, nats.ErrDisconnected):
		return AsyncPublishOutcomeUnknown
	case errors.Is(err, nats.ErrConnectionClosed):
		return AsyncPublishOutcomeUnknown
	default:
		return AsyncPublishOutcomeFailed
	}
}

func (p *Producer) handleAsyncFailure(result AsyncPublishResult, sentTime time.Time) {
	isError := result.Outcome != AsyncPublishOutcomeAcked
	incProducerSentCounter(result.Subject, isError)
	observeProducerAckWaitingTime(result.Subject, isError, time.Since(sentTime))
	incAsyncPublishOutcomeCounter(result.Subject, result.Outcome)
	observeAsyncPublishResolutionTime(result.Subject, result.Outcome, time.Since(sentTime))

	switch result.Outcome {
	case AsyncPublishOutcomeUnknown:
		if !p.closed.Load() {
			p.logger.Warn("async publish result is unknown; fallback skipped",
				zap.String("subject", result.Subject),
				zap.Error(result.Err),
			)
		}
	case AsyncPublishOutcomeFailed:
		p.logger.Error("async publish failed",
			zap.String("subject", result.Subject),
			zap.Error(result.Err),
		)
		if p.fallback != nil {
			p.fallbackCh <- fallbackRequest{
				subject: result.Subject,
				data:    result.Data,
			}
		}
	}

	p.handleAsyncResult(result)
}

func (p *Producer) handleAsyncResult(result AsyncPublishResult) {
	if p.asyncHandler != nil {
		p.asyncHandler(result)
	}

	if result.Outcome == AsyncPublishOutcomeAcked || p.errorHandler == nil || result.Err == nil {
		return
	}

	p.errorHandler(
		fmt.Errorf("async publish %s: %w", result.Outcome, result.Err),
		result.Subject,
		result.Data,
	)
}

func (p *Producer) runFallbackWorker() {
	if p.fallback == nil {
		return
	}

	for range p.fallbackConcurrency {
		p.fallbackWG.Go(func() {
			for req := range p.fallbackCh {
				ctx, cancel := context.WithTimeout(context.Background(), p.fallbackTimeout)
				err := p.fallback.SaveMessage(ctx, req.subject, req.data)
				cancel()
				if err != nil {
					p.logger.Error("failed to save to fallback",
						zap.String("subject", req.subject),
						zap.Error(err),
					)

					if p.errorHandler != nil {
						handlerErr := fmt.Errorf("failed to save to fallback: %w", err)
						p.errorHandler(handlerErr, req.subject, req.data)
					}
				}
			}
		})
	}
}
