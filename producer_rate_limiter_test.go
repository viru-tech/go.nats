package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestComputeThrottleStateFromStreamInfo_StreamFullOnlyAtCapacity(t *testing.T) {
	t.Parallel()

	cfg := BackpressureStreamConfig{
		ThresholdBytesPercent: 80,
		MaxDelayAtPercent:     95,
		MaxDelay:              time.Second,
	}

	delay, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo(&jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 100},
		State:  jetstream.StreamState{Bytes: 85},
	}, cfg)
	require.True(t, isThrottling)
	require.False(t, isStreamFull)
	require.Positive(t, delay)

	delay, isThrottling, isStreamFull = computeThrottleStateFromStreamInfo(&jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 100},
		State:  jetstream.StreamState{Bytes: 100},
	}, cfg)
	require.True(t, isThrottling)
	require.True(t, isStreamFull)
	require.Equal(t, cfg.MaxDelay, delay)
}

func TestProducerThrottle_UsesStreamNameBySubject(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	js := NewMockJetStream(ctrl)
	p := &Producer{
		js:     js,
		logger: zaptest.NewLogger(t),
		backPressure: map[string]*backpressureController{
			"ORDERS": {delay: 0},
		},
	}

	js.EXPECT().StreamNameBySubject(gomock.Any(), "orders.created").Return("ORDERS", nil)

	err := p.throttle(t.Context(), "orders.created")
	require.NoError(t, err)

	cached, ok := p.streamBySubject.Load("orders.created")
	require.True(t, ok)
	require.Equal(t, "ORDERS", cached)
}

func TestProducerProduceJSONAsync_AppliesBackpressure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	js := NewMockJetStream(ctrl)
	p := &Producer{
		js:     js,
		logger: zaptest.NewLogger(t),
		backPressure: map[string]*backpressureController{
			"ORDERS": {delay: 0},
		},
		ackCh: make(chan pubAckWithTime, 1),
	}

	msg := []byte(`{"key":"value"}`)
	js.EXPECT().StreamNameBySubject(gomock.Any(), "orders.created").Return("ORDERS", nil)
	js.EXPECT().PublishAsync("orders.created", msg).Return(nil, context.DeadlineExceeded)

	err := p.ProduceJSONAsync("orders.created", map[string]string{"key": "value"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestApplyBackpressure_StopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		logger: zaptest.NewLogger(t),
		backPressure: map[string]*backpressureController{
			"ORDERS": {},
		},
	}

	p.applyBackpressure(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.backPressureWG.Wait()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("backpressure goroutine did not stop after cancellation")
	}
}
