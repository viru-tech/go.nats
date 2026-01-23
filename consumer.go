package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	// defaultConcurrency how many goroutines will be used for message consumption.
	defaultConcurrency = 5

	// defaultAckWait how long the NATS server will wait before marking a message as not acknowledged.
	defaultAckWait = 5 * time.Second

	defaultMaxAckPending = 1_000

	defaultPullMaxMessages       = 500
	dfaultPullExpiry             = 30 * time.Second
	defaultPullThresholdMessages = 250
	defaultMaxWaiting            = 512
)

// Handler function for message processing.
type Handler func(ctx context.Context, msg jetstream.Msg) error

// Consumer is responsible for consuming notifications from queue.
type Consumer struct {
	jsCtx    jetstream.JetStream
	consumer jetstream.ConsumeContext

	logger     *zap.Logger
	connection *nats.Conn

	durable string
	subject string

	ackWait               time.Duration
	pullExpiry            time.Duration
	concurrency           int
	maxAckPending         int
	pullMaxMessages       int
	pullThresholdMessages int
	maxWaiting            int
	compression           bool
}

// NewConsumer creates new instance of Consumer.
func NewConsumer(urls []string, subject, durable string, opts ...ConsumerOption) (*Consumer, error) {
	consumer := &Consumer{
		subject:               subject,
		concurrency:           defaultConcurrency,
		durable:               durable,
		ackWait:               defaultAckWait,
		maxAckPending:         defaultMaxAckPending,
		pullMaxMessages:       defaultPullMaxMessages,
		pullExpiry:            dfaultPullExpiry,
		pullThresholdMessages: defaultPullThresholdMessages,
		maxWaiting:            defaultMaxWaiting,
	}
	for i := range opts {
		opts[i](consumer)
	}
	natsOpts := nats.Options{
		Servers:        urls,
		AllowReconnect: true,
		Compression:    consumer.compression,
		MaxReconnect:   -1,
		DisconnectedErrCB: func(_ *nats.Conn, err error) {
			if err != nil {
				consumer.logger.Error("connection to NATS lost, reconnecting...", zap.Error(err))
			}
		},
		ReconnectedCB: func(_ *nats.Conn) {
			consumer.logger.Info("connection to NATS restored")
		},
	}
	nc, err := natsOpts.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	consumer.connection = nc
	consumer.jsCtx, err = jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream instance: %w", err)
	}

	return consumer, nil
}

// Close closes the connection.
//
//nolint:unparam
func (c *Consumer) Close() error {
	if c.consumer != nil {
		c.consumer.Drain()
		<-c.consumer.Closed()
	}

	c.connection.Close()

	return nil
}

// Run creates a new pull consumer. This is a blocking call.
// To stop serving, cancel the passed context.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	consumer, err := c.jsCtx.CreateOrUpdateConsumer(ctx, c.subject, jetstream.ConsumerConfig{
		FilterSubjects: []string{
			c.subject,
		},
		Durable:       c.durable,
		AckWait:       c.ackWait,
		MaxAckPending: c.maxAckPending,
		MaxWaiting:    c.maxWaiting,
	})
	if err != nil {
		return err
	}

	workC := make(chan jetstream.Msg, c.concurrency)
	wg := new(sync.WaitGroup)
	wg.Add(c.concurrency)
	for range c.concurrency {
		go func() {
			defer wg.Done()
			for msg := range workC {
				c.handleMessageWithMetrics(ctx, msg, handler)
			}
		}()
	}

	consumeCtx, err := consumer.Consume(
		func(msg jetstream.Msg) {
			workC <- msg
		},
		jetstream.PullMaxMessages(c.pullMaxMessages),
		jetstream.PullExpiry(c.pullExpiry),
		jetstream.PullThresholdMessages(c.pullThresholdMessages),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if err != nil {
				c.logger.Error("NATS consumer error", zap.Error(err))
			}
		}),
	)

	defer func() {
		close(workC)
		wg.Wait()
	}()

	if err != nil {
		return err
	}
	c.consumer = consumeCtx

	select {
	case <-ctx.Done():
		consumeCtx.Stop()
		<-consumeCtx.Closed()

	case <-consumeCtx.Closed():
	}

	return nil
}

func (c *Consumer) handleMessageWithMetrics(ctx context.Context, msg jetstream.Msg, handler Handler) {
	var err error
	defer func(now time.Time) {
		observeConsumerHandlingTime(c.subject, err != nil, time.Since(now))
	}(time.Now())

	err = handler(ctx, msg)
}
