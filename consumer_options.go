package nats

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// ConsumerOption configures Consumer.
type ConsumerOption func(c *Consumer)

// WithConsumerLogger sets passed logger.
func WithConsumerLogger(l *zap.Logger) ConsumerOption {
	return func(c *Consumer) {
		c.logger = l
	}
}

// WithConcurrency how many goroutines will be used for message consumption.
// Defaults to 5.
func WithConcurrency(concurrency int) ConsumerOption {
	return func(c *Consumer) {
		c.concurrency = concurrency
	}
}

// WithConsumerCompression enables compression for consumer.
func WithConsumerCompression() ConsumerOption {
	return func(c *Consumer) {
		c.compression = true
	}
}

// WithMaxAckPending sets maximum in-flight messages.
// Defaults to 1000.
func WithMaxAckPending(maxAckPending int) ConsumerOption {
	return func(c *Consumer) {
		c.maxAckPending = maxAckPending
	}
}

// WithAckWait how long NATS server will be waiting for an ack.
// Defaults to 5 seconds.
func WithAckWait(ackWait time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.ackWait = ackWait
	}
}

// WithPullExpiry sets timeout on a single pull request, waiting until at least one message is available.
// If not provided, a default of 30 seconds will be used.
func WithPullExpiry(expiry time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.pullExpiry = expiry
	}
}

// WithPullMaxMessages limits the number of messages to be buffered in the client.
// If not provided, a default of 500 messages will be used.
func WithPullMaxMessages(maxMessages int) ConsumerOption {
	return func(c *Consumer) {
		c.pullMaxMessages = maxMessages
	}
}

// WithPullThresholdMessages sets the message count on which Consume will trigger new pull request to the server.
// Defaults to 250.
func WithPullThresholdMessages(threshold int) ConsumerOption {
	return func(c *Consumer) {
		c.pullThresholdMessages = threshold
	}
}

// WithMaxWaiting sets the maximum number of waiting pull requests.
// Defaults to 512.
func WithMaxWaiting(maxWaiting int) ConsumerOption {
	return func(c *Consumer) {
		c.maxWaiting = maxWaiting
	}
}

// WithAckPolicy sets consumer ack policy.
// Defaults to AckExplicit.
func WithAckPolicy(policy jetstream.AckPolicy) ConsumerOption {
	return func(c *Consumer) {
		c.ackPolicy = policy
	}
}
