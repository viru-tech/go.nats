package nats

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:gochecknoglobals
var (
	producerSentCounter    *prometheus.CounterVec
	fallbackSavedCounter   *prometheus.CounterVec
	resendUnprocessedGauge *prometheus.GaugeVec
	resendSentCounter      *prometheus.CounterVec
	consumerHandlingTime   *prometheus.HistogramVec
	producerAckWaitingTime *prometheus.HistogramVec
)

// MetricsOpts contains metrics configuration.
type MetricsOpts struct {
	ConstLabels map[string]string
}

// EnableProducerMetrics creates a set of metrics for a producer and registers on a
// Prometheus registry.
func EnableProducerMetrics(opts ...MetricsOption) {
	mOpts := &MetricsOpts{
		ConstLabels: make(map[string]string),
	}
	for _, o := range opts {
		o(mOpts)
	}

	producerSentCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "nats_producer_sent_total",
			Help:        "Total number of messages sent to the nats",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"subject", "error"},
	)
	fallbackSavedCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "nats_fallback_saved_total",
			Help:        "Total number of messages saved to nats",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type", "subject", "error"},
	)
	resendUnprocessedGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "nats_resend_unprocessed",
			Help:        "Number of messages that are currently unprocessed by nats",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type"},
	)
	resendSentCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "nats_resend_sent_total",
			Help:        "Total number of messages that were resent to the nats",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type"},
	)

	producerAckWaitingTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nats_producer_ack_waiting_duration_seconds",
			Help:    "Time in seconds it took for NATS to reply with an ack",
			Buckets: []float64{.05, .1, .2, .3, .5, 1, 2, 3, 5, 10},
		},
		[]string{"subject", "error"},
	)
}

// EnableConsumerMetrics creates a set of consumer metrics and registers on a
// Prometheus registry.
func EnableConsumerMetrics(opts ...MetricsOption) {
	mOpts := &MetricsOpts{
		ConstLabels: make(map[string]string),
	}
	for _, o := range opts {
		o(mOpts)
	}

	consumerHandlingTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nats_consumer_handling_duration_seconds",
			Help:    "Time in seconds of message handling",
			Buckets: []float64{.05, .1, .2, .3, .5, 1, 2, 3, 5, 10},
		},
		[]string{"subject", "error"},
	)
}

func incProducerSentCounter(subject string, isError bool) {
	if producerSentCounter != nil {
		producerSentCounter.WithLabelValues(subject, strconv.FormatBool(isError)).Inc()
	}
}

func observeProducerAckWaitingTime(subject string, isError bool, d time.Duration) {
	if producerAckWaitingTime != nil {
		producerAckWaitingTime.
			WithLabelValues(subject, strconv.FormatBool(isError)).
			Observe(d.Seconds())
	}
}

func incFallbackSavedCounter(fallbackType, subject string, isError bool) {
	if fallbackSavedCounter != nil {
		fallbackSavedCounter.WithLabelValues(fallbackType, subject, strconv.FormatBool(isError)).Inc()
	}
}

func setResendUnprocessedGauge(fallbackType string, value int) {
	if resendUnprocessedGauge != nil {
		resendUnprocessedGauge.WithLabelValues(fallbackType).Set(float64(value))
	}
}

func incResendSentCounter(fallbackType string) {
	if resendSentCounter != nil {
		resendSentCounter.WithLabelValues(fallbackType).Inc()
	}
}

// observeConsumerHandlingTime adds observation of message handling time to the metric.
func observeConsumerHandlingTime(subject string, isErr bool, d time.Duration) {
	if consumerHandlingTime != nil {
		consumerHandlingTime.
			WithLabelValues(subject, strconv.FormatBool(isErr)).
			Observe(d.Seconds())
	}
}
