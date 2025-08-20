package nats

import (
	"context"
	"fmt"
	"io"
	"sync"
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
	ProduceJSON(ctx context.Context, subject string, v interface{}) error
	// ProduceBytes publishes a []bytes to the specified subject.
	ProduceBytes(ctx context.Context, subject string, data []byte) error
	// ProduceJSONAsync publishes async a JSON message to the specified subject.
	ProduceJSONAsync(subject string, v interface{}) error
}

// ErrorHandler defines the error function that will be called when error is received.
type ErrorHandler func(err error, subject string, data []byte)

type pubAckWithTime struct {
	ackFuture jetstream.PubAckFuture
	sentTime  time.Time
}

// Producer is a NATS Producer that publishes messages to subjects.
type Producer struct {
	logger *zap.Logger
	nc     *nats.Conn
	js     jetstream.JetStream

	errorHandler ErrorHandler
	fallback     Fallback
	ackCh        chan pubAckWithTime
	fallbackCh   chan fallbackRequest
	fastime      fastime.Fastime

	ackWG               sync.WaitGroup
	fallbackWG          sync.WaitGroup
	fallbackTimeout     time.Duration
	publishTimeout      time.Duration
	ackBufferSize       int
	fallbackBufferSize  int
	ackConcurrency      int
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
		ackBufferSize:       10,
		fallbackBufferSize:  10,
		ackConcurrency:      1,
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

	p.js, err = jetstream.New(p.nc)
	if err != nil {
		p.nc.Close()
		return nil, fmt.Errorf("failed to create JetStream instance: %w", err)
	}

	p.ackCh = make(chan pubAckWithTime, p.ackBufferSize)
	p.fallbackCh = make(chan fallbackRequest, p.fallbackBufferSize)

	p.runAckWorker()
	p.runFallbackWorker()

	return p, nil
}

// Close closes the NATS connection.
func (p *Producer) Close() error { //nolint:unparam
	p.js.CleanupPublisher()
	if p.nc != nil {
		p.nc.Close()
	}
	close(p.ackCh)
	p.ackWG.Wait()
	close(p.fallbackCh)
	p.fallbackWG.Wait()
	return nil
}

// ProduceJSON publishes a JSON message to the specified subject.
func (p *Producer) ProduceJSON(ctx context.Context, subject string, v interface{}) error {
	data, err := jsoniter.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	return p.ProduceBytes(ctx, subject, data)
}

// ProduceJSONAsync publishes a JSON message to the specified subject asynchronously.
func (p *Producer) ProduceJSONAsync(subject string, v interface{}) error {
	var err error

	data, err := jsoniter.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	pubAckFuture, err := p.js.PublishAsync(subject, data)
	if err != nil {
		incProducerSentCounter(subject, true)

		if p.fallback != nil {
			p.fallbackCh <- fallbackRequest{
				subject: subject,
				data:    data,
			}
		}
		return err
	}

	p.ackCh <- pubAckWithTime{
		ackFuture: pubAckFuture,
		sentTime:  p.fastime.Now(),
	}
	return nil
}

// ProduceBytes publishes a []byte to the specified subject synchronously.
func (p *Producer) ProduceBytes(ctx context.Context, subject string, data []byte) error {
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

func (p *Producer) runAckWorker() {
	if p.ackCh == nil {
		return
	}
	for range p.ackConcurrency {
		p.ackWG.Add(1)
		go func() {
			defer p.ackWG.Done()

			for req := range p.ackCh {
				select {
				case ack := <-req.ackFuture.Ok():
					p.logger.Debug("message acknowledged",
						zap.String("stream", ack.Stream),
						zap.String("subject", req.ackFuture.Msg().Subject),
						zap.Uint64("sequence", ack.Sequence),
					)
					incProducerSentCounter(req.ackFuture.Msg().Subject, false)
					observeProducerAckWaitingTime(req.ackFuture.Msg().Subject, false, time.Since(req.sentTime))

				case err := <-req.ackFuture.Err():
					p.logger.Error("failed to publish",
						zap.String("subject", req.ackFuture.Msg().Subject),
						zap.Error(err),
					)
					incProducerSentCounter(req.ackFuture.Msg().Subject, true)
					observeProducerAckWaitingTime(req.ackFuture.Msg().Subject, true, time.Since(req.sentTime))
					if p.errorHandler != nil {
						handlerErr := fmt.Errorf("failed to publish: %w", err)
						p.errorHandler(handlerErr, req.ackFuture.Msg().Subject, req.ackFuture.Msg().Data)
					}

					if p.fallback != nil {
						p.fallbackCh <- fallbackRequest{
							subject: req.ackFuture.Msg().Subject,
							data:    req.ackFuture.Msg().Data,
						}
					}
				}
			}
		}()
	}
}

func (p *Producer) runFallbackWorker() {
	if p.fallback == nil {
		return
	}

	for range p.fallbackConcurrency {
		p.fallbackWG.Add(1)
		go func() {
			defer p.fallbackWG.Done()

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
		}()
	}
}
