package nats

import (
	"context"
	"fmt"
	"os"

	"github.com/hashicorp/go-multierror"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

//go:generate mockgen -source=fallback.go -package=nats -destination=fallback_mock.go

const fsFallbackType = "fs"

// Fallback is used when Nats is no longer available.
// ProducerNats will use fallback to persist message it could
// not have sent due to Nats failure.
type Fallback interface {
	// SaveMessage saves passed message somewhere so that
	// it is not lost due to Nats failure.
	SaveMessage(ctx context.Context, subject string, msg []byte) error
}

// FallbackChain allows chaining fallbacks so that if preceding
// fallback returns an error, next one will be used. If all fallbacks
// in a chain fail, FallbackChain generates its own error.
type FallbackChain []Fallback

// SaveMessage attempts to apply fallback mechanisms in the chain
// in order. If any of fallback succeeds no error is returned.
func (fc FallbackChain) SaveMessage(ctx context.Context, subject string, msg []byte) error {
	var multiErr error
	for _, f := range fc {
		if err := f.SaveMessage(ctx, subject, msg); err != nil {
			multiErr = multierror.Append(multiErr, err)
		} else {
			return nil
		}
	}

	return fmt.Errorf("failed to save message in fallback chain: %w", multiErr)
}

// FallbackMessage is what we persist on filesystem instead of msg.
type FallbackMessage struct {
	Subject string `json:"subject"`
	Data    []byte `json:"data"`
}

// NewFallbackMessage constructs FallbackMessage from the passed msg.
func NewFallbackMessage(subject string, data []byte) *FallbackMessage {
	return &FallbackMessage{
		Subject: subject,
		Data:    data,
	}
}

// FSFallback is a filesystem fallback. It persists messages
// as a separate files in the base directory, pointed by
// FSFallback string value. It automatically tries to resend
// saved messages once in a while.
type FSFallback struct {
	logger *zap.Logger

	dir string
}

// NewFSFallback returns new filesystem fallback, that will save passed messages
// as a separate files in dir and optionally resend them (if configured).
func NewFSFallback(dir string, opts ...FSFallbackOption) *FSFallback {
	f := &FSFallback{
		dir:    dir,
		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(f)
	}

	return f
}

// SaveMessage saves JSON-representation of the passed message in a new file with unique name.
func (ff *FSFallback) SaveMessage(_ context.Context, subject string, msg []byte) error {
	var err error
	defer func() {
		incFallbackSavedCounter(fsFallbackType, subject, err != nil)
	}()

	data := NewFallbackMessage(subject, msg)

	out, err := os.CreateTemp(ff.dir, "")
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close() //nolint:errcheck

	if err = jsoniter.NewEncoder(out).Encode(data); err != nil {
		return fmt.Errorf("failed to encode message to file: %w", err)
	}

	if err := out.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	ff.logger.Debug("saved nats message", zap.String("subject", subject), zap.String("file", out.Name()))

	return nil
}
