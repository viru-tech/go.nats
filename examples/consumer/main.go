package main

import (
	"context"
	"log"

	"github.com/nats-io/nats.go/jetstream"
	nats "github.com/viru-tech/go.nats"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := nats.NewConsumer(
		[]string{"nats://localhost:4222"},
		"my_test_subject",
		"my_consumer",
		nats.WithConsumerLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create nats consumer", zap.Error(err))
		return
	}

	defer consumer.Close() //nolint:errcheck

	err = consumer.Run(context.Background(), func(_ context.Context, msg jetstream.Msg) error {
		logger.Info(string(msg.Data()))
		err := msg.Ack()
		if err != nil {
			logger.Error("failed to ack msg", zap.Error(err))
		}
		return nil
	})
	if err != nil {
		log.Print(err)
	}
}
