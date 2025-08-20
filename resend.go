package nats

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

const (
	defaultRetryInterval = time.Minute
	defaultSendInterval  = 4 * time.Millisecond // 250rps
)

// FSResend is a filesystem resend.
type FSResend struct {
	logger *zap.Logger

	done          chan struct{}
	retryP        ProducerNats
	dir           string
	wg            sync.WaitGroup
	retryInterval time.Duration
	sendInterval  time.Duration
}

// NewFSResend returns new filesystem resend, that will resend saved messages
// in file from filesystem fallback.
// NB! Be careful with configuring fallback for producer p.
// If FSResend receives an error from producer,
// the file will be skipped and re-processed later.
// FSResend produces messages synchronously.
func NewFSResend(dir string, p ProducerNats, opts ...FSResendOption) *FSResend {
	f := &FSResend{
		dir:           dir,
		logger:        zap.NewNop(),
		done:          make(chan struct{}),
		retryInterval: defaultRetryInterval,
		sendInterval:  defaultSendInterval,
		retryP:        p,
	}

	for _, opt := range opts {
		opt(f)
	}

	return f
}

// Close stops background retry goroutine.
func (f *FSResend) Close() error { //nolint:unparam
	close(f.done)
	f.wg.Wait()
	return nil
}

// Run starts background retry goroutine.
func (f *FSResend) Run() {
	f.wg.Add(1)

	go func() {
		defer f.wg.Done()

		t := time.NewTicker(f.retryInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				if err := f.Resend(); err != nil {
					f.logger.Error("failed to resend messages", zap.Error(err))
				}
			case <-f.done:
				return
			}
		}
	}()
}

// Resend immediately run scan fallback directory and try to send messages.
func (f *FSResend) Resend() error {
	dd, err := os.ReadDir(f.dir)
	if err != nil {
		return fmt.Errorf("failed to read contents of %q: %w", f.dir, err)
	}
	setResendUnprocessedGauge(fsFallbackType, len(dd))

	t := time.NewTicker(f.sendInterval)
	defer t.Stop()

	for _, d := range dd {
		select {
		case <-t.C:
			if d.IsDir() {
				continue
			}

			path := filepath.Join(f.dir, d.Name())
			data, err := os.ReadFile(filepath.Clean(path))
			if err != nil {
				f.logger.Error("failed to read file, skipping", zap.String("path", path), zap.Error(err))
				continue
			}

			var msg FallbackMessage
			if err := jsoniter.Unmarshal(data, &msg); err != nil {
				f.logger.Error("failed to unmarshal message, skipping", zap.String("path", path), zap.Error(err))
				continue
			}

			err = f.retryP.ProduceBytes(context.Background(), msg.Subject, msg.Data)
			if err != nil {
				f.logger.Error("failed to produce message, skipping", zap.String("path", path), zap.Error(err))
				continue
			}
			incResendSentCounter(fsFallbackType)

			if err := os.Remove(path); err != nil {
				f.logger.Error("failed to remove file after resend", zap.String("path", path), zap.Error(err))
			}
		case <-f.done:
			return nil
		}
	}

	dd, err = os.ReadDir(f.dir)
	if err != nil {
		f.logger.Warn(fmt.Sprintf("failed to read contents of %q", f.dir), zap.Error(err))
		return nil
	}

	setResendUnprocessedGauge(fsFallbackType, len(dd))
	return nil
}
