package nats

import (
	"context"
	"errors"
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
					fallbackConcurrency: 1,
					fallbackCh:          make(chan fallbackRequest, 2),
					closedCh:            make(chan struct{}),
					fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
				}
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
					fallbackConcurrency: 1,
					fallbackCh:          make(chan fallbackRequest, 2),
					closedCh:            make(chan struct{}),
					fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
				}
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

	t.Run("async publish success", func(t *testing.T) { //nolint:paralleltest
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fb := NewMockFallback(ctrl)
		js := NewMockJetStream(ctrl)
		pa := NewMockPubAckFuture(ctrl)
		msg := []byte(`{"key":"value"}`)
		ackChan := make(chan *jetstream.PubAck, 1)
		errChan := make(chan error)
		doneCh := make(chan struct{})
		p := &Producer{
			js:                  js,
			fallback:            fb,
			logger:              zaptest.NewLogger(t),
			publishTimeout:      time.Second,
			fallbackTimeout:     time.Second,
			fallbackConcurrency: 1,
			fallbackCh:          make(chan fallbackRequest, 2),
			closedCh:            make(chan struct{}),
			fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
			asyncHandler: func(result AsyncPublishResult) {
				if result.Outcome == AsyncPublishOutcomeAcked {
					close(doneCh)
				}
			},
		}
		p.runFallbackWorker()
		defer p.Close() //nolint:errcheck

		js.EXPECT().CleanupPublisher()
		js.EXPECT().PublishAsync(subject, msg).Return(pa, nil)
		pa.EXPECT().Msg().Return(&nats.Msg{Subject: subject, Data: msg}).AnyTimes()
		pa.EXPECT().Ok().Return(ackChan)
		pa.EXPECT().Err().Return(errChan)

		ackChan <- &jetstream.PubAck{Stream: "test-stream", Sequence: 1}

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)
		select {
		case <-doneCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for acked outcome")
		}
	})

	t.Run("async immediate error", func(t *testing.T) { //nolint:paralleltest
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fb := NewMockFallback(ctrl)
		js := NewMockJetStream(ctrl)
		msg := []byte(`{"key":"value"}`)
		doneCh := make(chan AsyncPublishResult, 1)
		fallbackDone := make(chan struct{})
		p := &Producer{
			js:                  js,
			fallback:            fb,
			logger:              zaptest.NewLogger(t),
			publishTimeout:      time.Second,
			fallbackTimeout:     time.Second,
			fallbackConcurrency: 1,
			fallbackCh:          make(chan fallbackRequest, 2),
			closedCh:            make(chan struct{}),
			fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
			asyncHandler: func(result AsyncPublishResult) {
				doneCh <- result
			},
		}
		p.runFallbackWorker()
		defer p.Close() //nolint:errcheck

		js.EXPECT().CleanupPublisher()
		js.EXPECT().PublishAsync(subject, msg).Return(nil, errors.New("async error"))

		fb.EXPECT().SaveMessage(gomock.Any(), subject, msg).Return(nil).
			Do(func(context.Context, string, []byte) {
				close(fallbackDone)
			})

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.ErrorContains(t, err, "async error")

		select {
		case result := <-doneCh:
			require.Equal(t, AsyncPublishOutcomeFailed, result.Outcome)
			require.ErrorContains(t, result.Err, "async error")
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for failed outcome")
		}
		select {
		case <-fallbackDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for fallback save")
		}
	})

	t.Run("async ack error with fallback", func(t *testing.T) { //nolint:paralleltest
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fb := NewMockFallback(ctrl)
		js := NewMockJetStream(ctrl)
		pa := NewMockPubAckFuture(ctrl)
		msg := []byte(`{"key":"value"}`)
		ackChan := make(chan *jetstream.PubAck)
		errChan := make(chan error, 1)
		doneCh := make(chan AsyncPublishResult, 1)
		fallbackDone := make(chan struct{})
		p := &Producer{
			js:                  js,
			fallback:            fb,
			logger:              zaptest.NewLogger(t),
			publishTimeout:      time.Second,
			fallbackTimeout:     time.Second,
			fallbackConcurrency: 1,
			fallbackCh:          make(chan fallbackRequest, 2),
			closedCh:            make(chan struct{}),
			fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
			asyncHandler: func(result AsyncPublishResult) {
				doneCh <- result
			},
		}
		p.runFallbackWorker()
		defer p.Close() //nolint:errcheck

		js.EXPECT().CleanupPublisher()
		js.EXPECT().PublishAsync(subject, msg).Return(pa, nil)
		pa.EXPECT().Msg().Return(&nats.Msg{Subject: subject, Data: msg}).AnyTimes()
		pa.EXPECT().Ok().Return(ackChan)
		pa.EXPECT().Err().Return(errChan)

		fb.EXPECT().SaveMessage(gomock.Any(), subject, msg).Return(nil).
			Do(func(context.Context, string, []byte) {
				close(fallbackDone)
			})

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)

		errChan <- errors.New("some ack error")

		select {
		case result := <-doneCh:
			require.Equal(t, AsyncPublishOutcomeFailed, result.Outcome)
			require.ErrorContains(t, result.Err, "some ack error")
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for failed outcome")
		}
		select {
		case <-fallbackDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for fallback save")
		}
	})

	t.Run("async timeout becomes unknown and skips fallback", func(t *testing.T) { //nolint:paralleltest
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fb := NewMockFallback(ctrl)
		js := NewMockJetStream(ctrl)
		pa := NewMockPubAckFuture(ctrl)
		msg := []byte(`{"key":"value"}`)
		ackChan := make(chan *jetstream.PubAck)
		errChan := make(chan error, 1)
		doneCh := make(chan AsyncPublishResult, 1)
		p := &Producer{
			js:                  js,
			fallback:            fb,
			logger:              zaptest.NewLogger(t),
			publishTimeout:      time.Second,
			fallbackTimeout:     time.Second,
			fallbackConcurrency: 1,
			fallbackCh:          make(chan fallbackRequest, 2),
			closedCh:            make(chan struct{}),
			fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
			asyncHandler: func(result AsyncPublishResult) {
				doneCh <- result
			},
		}
		p.runFallbackWorker()
		defer p.Close() //nolint:errcheck

		js.EXPECT().CleanupPublisher()
		js.EXPECT().PublishAsync(subject, msg).Return(pa, nil)
		pa.EXPECT().Msg().Return(&nats.Msg{Subject: subject, Data: msg}).AnyTimes()
		pa.EXPECT().Ok().Return(ackChan)
		pa.EXPECT().Err().Return(errChan)

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)

		errChan <- jetstream.ErrAsyncPublishTimeout

		select {
		case result := <-doneCh:
			require.Equal(t, AsyncPublishOutcomeUnknown, result.Outcome)
			require.ErrorIs(t, result.Err, jetstream.ErrAsyncPublishTimeout)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for unknown outcome")
		}
	})

	t.Run("one unresolved future does not block other async results", func(t *testing.T) { //nolint:paralleltest
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		fb := NewMockFallback(ctrl)
		js := NewMockJetStream(ctrl)
		stuckFuture := NewMockPubAckFuture(ctrl)
		resolvedFuture := NewMockPubAckFuture(ctrl)
		msg := []byte(`{"key":"value"}`)
		stuckAckCh := make(chan *jetstream.PubAck)
		stuckErrCh := make(chan error, 1)
		resolvedAckCh := make(chan *jetstream.PubAck)
		resolvedErrCh := make(chan error, 1)
		resolvedDone := make(chan AsyncPublishResult, 1)
		p := &Producer{
			js:                  js,
			fallback:            fb,
			logger:              zaptest.NewLogger(t),
			publishTimeout:      time.Second,
			fallbackTimeout:     time.Second,
			fallbackConcurrency: 1,
			fallbackCh:          make(chan fallbackRequest, 2),
			closedCh:            make(chan struct{}),
			fastime:             fastime.New().StartTimerD(t.Context(), time.Millisecond*5),
			asyncHandler: func(result AsyncPublishResult) {
				if result.Subject == subject && result.Outcome == AsyncPublishOutcomeUnknown {
					resolvedDone <- result
				}
			},
		}
		p.runFallbackWorker()
		defer p.Close() //nolint:errcheck

		js.EXPECT().CleanupPublisher()
		js.EXPECT().PublishAsync(subject, msg).Return(stuckFuture, nil)
		js.EXPECT().PublishAsync(subject, msg).Return(resolvedFuture, nil)

		stuckFuture.EXPECT().Msg().Return(&nats.Msg{Subject: subject, Data: msg}).AnyTimes()
		stuckFuture.EXPECT().Ok().Return(stuckAckCh).AnyTimes()
		stuckFuture.EXPECT().Err().Return(stuckErrCh).AnyTimes()

		resolvedFuture.EXPECT().Msg().Return(&nats.Msg{Subject: subject, Data: msg}).AnyTimes()
		resolvedFuture.EXPECT().Ok().Return(resolvedAckCh)
		resolvedFuture.EXPECT().Err().Return(resolvedErrCh)

		err := p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)
		err = p.ProduceJSONAsync(subject, map[string]string{"key": "value"})
		require.NoError(t, err)

		resolvedErrCh <- nats.ErrDisconnected

		select {
		case result := <-resolvedDone:
			require.Equal(t, AsyncPublishOutcomeUnknown, result.Outcome)
			require.ErrorIs(t, result.Err, nats.ErrDisconnected)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for independent async resolution")
		}

		stuckErrCh <- jetstream.ErrAsyncPublishTimeout
	})
}
