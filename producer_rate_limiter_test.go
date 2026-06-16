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

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			ThresholdBytesPercent: 80,
			MaxDelay:              time.Second,
			MaxBytes:              100,
		},
	}

	delay, _, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 999},
		State:  jetstream.StreamState{Bytes: 85},
	}, ctrl)
	require.True(t, isThrottling)
	require.False(t, isStreamFull)
	require.Positive(t, delay)

	delay, _, isThrottling, isStreamFull = computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 999},
		State:  jetstream.StreamState{Bytes: 100},
	}, ctrl)
	require.True(t, isThrottling)
	require.True(t, isStreamFull)
	require.Equal(t, ctrl.config.MaxDelay, delay)
}

func TestThrottleFactor_DefaultEaseInOutStartsSlowerThanLinear(t *testing.T) {
	t.Parallel()

	fill := 0.84
	threshold := 0.80

	easeInOut := throttleFactor(fill, threshold)
	linear := (fill - threshold) / (1 - threshold)

	require.Greater(t, easeInOut, 0.0)
	require.Less(t, easeInOut, linear)
}

func TestComputeThrottleStateFromStreamInfo_UsesConfiguredMaxesInThrottlePolicy(t *testing.T) {
	t.Parallel()

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			MaxDelay:              time.Second,
			ThresholdBytesPercent: 80,
			MaxBytes:              200,
			ThrottlePolicy: func(input ThrottlePolicyInput) ThrottlePolicyResult {
				require.Equal(t, "ORDERS", input.StreamName)
				require.InDelta(t, 0.425, input.BytesFillRatio, 0.0001)
				require.InDelta(t, 0.80, input.ThresholdBytesRatio, 0.0001)
				return ThrottlePolicyResult{Factor: 0.25}
			},
		},
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 100},
		State:  jetstream.StreamState{Bytes: 85},
	}, ctrl)
	require.True(t, isThrottling)
	require.False(t, isStreamFull)
	require.Equal(t, 250*time.Millisecond, delay)
	require.InDelta(t, 0.25, factor, 0.0001)
}

func TestComputeThrottleStateFromStreamInfo_UsesConfiguredMaxMsgs(t *testing.T) {
	t.Parallel()

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			MaxDelay:             time.Second,
			ThresholdMsgsPercent: 80,
			MaxMsgs:              200,
		},
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxMsgs: 100},
		State:  jetstream.StreamState{Msgs: 170},
	}, ctrl)
	require.True(t, isThrottling)
	require.False(t, isStreamFull)
	require.Positive(t, delay)
	require.Positive(t, factor)
}

func TestComputeThrottleStateFromStreamInfo_DisablesBytesWithoutConfiguredMax(t *testing.T) {
	t.Parallel()

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			MaxDelay:              time.Second,
			ThresholdBytesPercent: 80,
		},
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 100},
		State:  jetstream.StreamState{Bytes: 95},
	}, ctrl)
	require.Zero(t, delay)
	require.Zero(t, factor)
	require.False(t, isThrottling)
	require.False(t, isStreamFull)
}

func TestComputeThrottleStateFromStreamInfo_DisablesMsgsWithoutConfiguredMax(t *testing.T) {
	t.Parallel()

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			MaxDelay:             time.Second,
			ThresholdMsgsPercent: 80,
		},
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxMsgs: 100},
		State:  jetstream.StreamState{Msgs: 95},
	}, ctrl)
	require.Zero(t, delay)
	require.Zero(t, factor)
	require.False(t, isThrottling)
	require.False(t, isStreamFull)
}

func TestComputeThrottleStateFromStreamInfo_IgnoresStreamConfigLimits(t *testing.T) {
	t.Parallel()

	ctrl := &backpressureController{
		config: BackpressureStreamConfig{
			MaxDelay:              time.Second,
			ThresholdBytesPercent: 80,
			MaxBytes:              100,
		},
	}

	delay, factor, isThrottling, isStreamFull := computeThrottleStateFromStreamInfo("ORDERS", &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 1000},
		State:  jetstream.StreamState{Bytes: 850},
	}, ctrl)
	require.True(t, isThrottling)
	require.True(t, isStreamFull)
	require.Equal(t, time.Second, delay)
	require.InDelta(t, 1.0, factor, 0.0001)
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

type fauxStream struct {
	jetstream.Stream
}

func (fauxStream) Info(_ context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: 100},
		State:  jetstream.StreamState{Bytes: 85},
	}, nil
}

func TestApplyBackpressure_StopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockJetStream(ctrl)

	mock.EXPECT().Stream(gomock.Any(), "ORDERS").Return(
		jetstream.Stream(fauxStream{}),
		nil,
	)

	p := &Producer{
		logger: zaptest.NewLogger(t),
		backPressure: map[string]*backpressureController{
			"ORDERS": {},
		},
		js: mock,
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
