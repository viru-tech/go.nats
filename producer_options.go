package nats

import (
	"time"

	"go.uber.org/zap"
)

// ProducerOption configures Producer.
type ProducerOption func(p *Producer)

// WithProducerLogger sets passed logger.
func WithProducerLogger(l *zap.Logger) ProducerOption {
	return func(p *Producer) {
		p.logger = l
	}
}

// WithNATSTimeoutOption sets the timeout for the NATS JetStream Publish call.
func WithNATSTimeoutOption(t time.Duration) ProducerOption {
	return func(p *Producer) {
		p.publishTimeout = t
	}
}

// WithProducerCompression enables compression for producer.
func WithProducerCompression() ProducerOption {
	return func(p *Producer) {
		p.compression = true
	}
}

// WithErrorHandler sets the error handler function that will be called
// when an error is received during asynchronous sending or fallback.
func WithErrorHandler(handler ErrorHandler) ProducerOption {
	return func(p *Producer) {
		p.errorHandler = handler
	}
}

// WithAsyncOutcomeHandler sets a structured handler for terminal async publish results.
func WithAsyncOutcomeHandler(handler AsyncOutcomeHandler) ProducerOption {
	return func(p *Producer) {
		p.asyncHandler = handler
	}
}

// WithPublishAsyncTimeout sets the maximum time to wait for an async publish acknowledgement.
func WithPublishAsyncTimeout(t time.Duration) ProducerOption {
	return func(p *Producer) {
		p.asyncAckTimeout = t
	}
}

// WithPublishAsyncMaxPending sets the maximum number of outstanding async publishes.
func WithPublishAsyncMaxPending(n int) ProducerOption {
	return func(p *Producer) {
		p.asyncMaxPending = n
	}
}

// WithFallback sets passed fallback.
func WithFallback(f Fallback) ProducerOption {
	return func(p *Producer) {
		p.fallback = f
	}
}

// WithFallbackTimeout sets timeout for the fallback.
func WithFallbackTimeout(t time.Duration) ProducerOption {
	return func(p *Producer) {
		p.fallbackTimeout = t
	}
}

// WithFallbackBufferSize sets buffer size for the fallback channel.
func WithFallbackBufferSize(size int) ProducerOption {
	return func(p *Producer) {
		p.fallbackBufferSize = size
	}
}

// WithFallbackConcurrency sets the number of concurrency for processing fallback.
func WithFallbackConcurrency(number int) ProducerOption {
	return func(p *Producer) {
		p.fallbackConcurrency = number
	}
}

// WithFallbackChain creates FallbackChain from the passed
// Fallback slice and sets it as a fallback for the Producer.
func WithFallbackChain(ff ...Fallback) ProducerOption {
	return func(p *Producer) {
		p.fallback = FallbackChain(ff)
	}
}

// WithBackPressureConfig sets per-stream producer backpressure thresholds and callbacks.
func WithBackPressureConfig(config BackpressureConfig) ProducerOption {
	return func(p *Producer) {
		controllers := make(map[string]*backpressureController)
		for stream := range config {
			controllers[stream] = &backpressureController{
				config: config[stream],
			}
		}
		p.backPressure = controllers
	}
}

// FSFallbackOption configures FSFallback.
type FSFallbackOption func(f *FSFallback)

// WithFSFallbackLogger sets passed logger.
func WithFSFallbackLogger(l *zap.Logger) FSFallbackOption {
	return func(f *FSFallback) {
		f.logger = l
	}
}

// FSResendOption configures FSResend.
type FSResendOption func(f *FSResend)

// WithFSResendRetryInterval sets interval between retries.
func WithFSResendRetryInterval(d time.Duration) FSResendOption {
	return func(f *FSResend) {
		f.retryInterval = d
	}
}

// WithFSSendInterval sets interval between sending messages
// during one iteration of retry.
func WithFSSendInterval(d time.Duration) FSResendOption {
	return func(f *FSResend) {
		f.sendInterval = d
	}
}

// WithFSResendLogger sets passed logger.
func WithFSResendLogger(l *zap.Logger) FSResendOption {
	return func(f *FSResend) {
		f.logger = l
	}
}

// MetricsOption configures MetricsOpts.
type MetricsOption func(o *MetricsOpts)

// WithMetricsConstLabel sets passed label as const for all metrics.
func WithMetricsConstLabel(label, value string) MetricsOption {
	return func(o *MetricsOpts) {
		o.ConstLabels[label] = value
	}
}
