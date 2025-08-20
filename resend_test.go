package nats

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestFSFallback_resendMessages(t *testing.T) {
	t.Parallel()

	const retryInterval = time.Millisecond * 100
	msg := []byte(`{"key":"value"}`)
	subject := "subject"

	dir := t.TempDir()
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err)
	})

	ctrl := gomock.NewController(t)
	p := NewMockProducerNats(ctrl)
	p.EXPECT().ProduceBytes(gomock.Any(), subject, msg)

	fsFallback := NewFSFallback(dir)

	fsResend := NewFSResend(dir, p, WithFSResendRetryInterval(retryInterval))
	t.Cleanup(func() {
		require.NoError(t, fsResend.Close())
	})
	fsResend.Run()

	err := fsFallback.SaveMessage(t.Context(), subject, msg)
	require.NoError(t, err)

	dd, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, dd, 1)

	time.Sleep(2 * retryInterval)
	dd, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, dd)
}
