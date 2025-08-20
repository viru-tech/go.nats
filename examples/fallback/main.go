package main

import (
	"context"
	"log"
	"time"

	nats "github.com/viru-tech/go.nats"
	"go.uber.org/zap"
)

type message struct {
	Data string `json:"data"`
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	fallbackProducer, err := nats.NewProducer(
		[]string{"nats://localhost:4222"},
		nats.WithProducerLogger(logger),
		nats.WithFallbackTimeout(2*time.Second),
	)
	if err != nil {
		logger.Error("failed to create fallback nats producer", zap.Error(err))
		return
	}

	fsFallback := nats.NewFSFallback(
		"/tmp/nats_fallback",
		nats.WithFSFallbackLogger(logger),
	)

	resendProducer, err := nats.NewProducer(
		[]string{"nats://localhost:4222"},
		nats.WithProducerLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create resend nats producer", zap.Error(err))
		return
	}

	fsResend := nats.NewFSResend(
		"/tmp/nats_fallback",
		resendProducer,
		nats.WithFSResendRetryInterval(time.Minute),
		nats.WithFSResendLogger(logger),
	)
	fsResend.Run()
	defer fsResend.Close() //nolint:errcheck

	mainProducer, err := nats.NewProducer(
		[]string{"nats://localhost:4222"},
		nats.WithProducerLogger(logger),
		nats.WithFallbackChain(fallbackProducer, fsFallback),
		nats.WithFallbackTimeout(2*time.Second),
	)
	if err != nil {
		logger.Error("failed to create main nats producer", zap.Error(err))
		return
	}
	defer mainProducer.Close() //nolint:errcheck

	msg := message{Data: "Hello, NATS with fallback!"}
	if err := mainProducer.ProduceJSON(context.Background(), "my_test_subject", &msg); err != nil {
		logger.Error("failed to send json with mainProducer", zap.Error(err))
	}
}
