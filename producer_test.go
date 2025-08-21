package nats

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"github.com/viru-tech/fastime/v2"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	subject = "test.subject"
)

func TestProducer_ProduceJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(*testing.T)
		name string
	}{
		{
			name: "successful publish",
			test: func(t *testing.T) {
				t.Helper()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				js := NewMockJetStream(ctrl)
				fb := NewMockFallback(ctrl)
				logger := zaptest.NewLogger(t)

				p := &Producer{
					js:                  js,
					fallback:            fb,
					logger:              logger,
					publishTimeout:      time.Second,
					fallbackTimeout:     time.Second,
					ackConcurrency:      1,
					fallbackConcurrency: 1,
					ackCh:               make(chan pubAckWithTime, 2),
					fallbackCh:          make(chan fallbackRequest, 2),
					fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
				}
				p.runAckWorker()
				defer p.Close() //nolint:errcheck
				p.runFallbackWorker()

				msg := []byte(`{"key":"value"}`)
				js.EXPECT().Publish(gomock.Any(), subject, msg).Return(nil, nil)
				js.EXPECT().CleanupPublisher()

				err := p.ProduceJSON(t.Context(), subject, map[string]string{"key": "value"})
				require.NoError(t, err)
			},
		},
		{
			name: "marshal error",
			test: func(t *testing.T) {
				t.Helper()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				logger := zaptest.NewLogger(t)
				p := &Producer{
					logger: logger,
				}

				err := p.ProduceJSON(t.Context(), "test.subject", make(chan int))
				require.ErrorContains(t, err, "failed to marshal data to JSON")
			},
		},
		{
			name: "publish error with fallback",
			test: func(t *testing.T) {
				t.Helper()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				js := NewMockJetStream(ctrl)
				fb := NewMockFallback(ctrl)
				logger := zaptest.NewLogger(t)

				p := &Producer{
					js:                  js,
					fallback:            fb,
					logger:              logger,
					publishTimeout:      time.Second,
					fallbackTimeout:     time.Second,
					ackConcurrency:      1,
					fallbackConcurrency: 1,
					ackCh:               make(chan pubAckWithTime, 2),
					fallbackCh:          make(chan fallbackRequest, 2),
					fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
				}
				p.runAckWorker()
				defer p.Close() //nolint:errcheck
				p.runFallbackWorker()

				msg := []byte(`{"key":"value"}`)
				js.EXPECT().Publish(gomock.Any(), subject, msg).Return(nil, errors.New("nats error"))
				fb.EXPECT().SaveMessage(gomock.Any(), subject, msg).Return(nil)
				js.EXPECT().CleanupPublisher()

				err := p.ProduceJSON(t.Context(), subject, map[string]string{"key": "value"})
				require.ErrorContains(t, err, "nats error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.test(t)
		})
	}
}

//nolint:tparallel
func TestProducer_ProduceJSONAsync(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fb := NewMockFallback(ctrl)
	js := NewMockJetStream(ctrl)
	pa := NewMockPubAckFuture(ctrl)
	logger := zaptest.NewLogger(t)

	msg := []byte(`{"key":"value"}`)

	p := &Producer{
		js:              js,
		fallback:        fb,
		logger:          logger,
		publishTimeout:  time.Second,
		fallbackTimeout: time.Second,

		ackConcurrency:      1,
		fallbackConcurrency: 1,
		ackCh:               make(chan pubAckWithTime, 2),
		fallbackCh:          make(chan fallbackRequest, 2),
		fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
	}
	p.runAckWorker()
	p.runFallbackWorker()

	ackChan := make(chan *jetstream.PubAck, 1)
	errChan := make(chan error, 1)

	ack := &jetstream.PubAck{
		Stream:   "test-stream",
		Sequence: 1,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	pa.EXPECT().Msg().Return(&nats.Msg{
		Subject: subject,
		Data:    msg,
	}).AnyTimes()

	t.Run("async publish success", func(t *testing.T) { //nolint:paralleltest
		js.EXPECT().PublishAsync(subject, msg).Return(pa, nil)

		pa.EXPECT().Ok().Return(ackChan).Do(func() {
			defer wg.Done()
		})
		pa.EXPECT().Err().Return(errChan)

		ackChan <- ack

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)

		wg.Wait()
	})

	t.Run("async immediate error", func(t *testing.T) { //nolint:paralleltest
		js.EXPECT().PublishAsync(subject, msg).Return(nil, errors.New("async error"))

		doneCh := make(chan struct{})
		fb.EXPECT().SaveMessage(gomock.Any(), subject, msg).Return(nil).
			Do(func(context.Context, string, []byte) {
				close(doneCh)
			})

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.ErrorContains(t, err, "async error")

		<-doneCh
	})

	t.Run("async ack error with fallback", func(t *testing.T) { //nolint:paralleltest
		js.EXPECT().PublishAsync(subject, msg).Return(pa, nil)

		pa.EXPECT().Ok().Return(ackChan)
		pa.EXPECT().Err().Return(errChan)

		doneCh := make(chan struct{})
		fb.EXPECT().SaveMessage(gomock.Any(), subject, msg).Return(nil).
			Do(func(context.Context, string, []byte) {
				close(doneCh)
			})

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)

		errChan <- errors.New("some ack error")

		<-doneCh
	})
}
