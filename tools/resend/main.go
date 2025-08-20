package main

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
	nats "github.com/viru-tech/go.nats"
	"go.uber.org/zap"
)

func main() {
	var config appConfig

	if _, err := flags.Parse(&config); err != nil {
		var flagsErr *flags.Error
		if errors.As(err, &flagsErr) && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		log.Fatalf("failed to parse config: %v", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync() //nolint:errcheck

	resendProducer, err := nats.NewProducer(
		config.NATS.URLs,
		nats.WithProducerLogger(logger),
		nats.WithFallbackTimeout(2*time.Second),
	)
	if err != nil {
		logger.Error("failed to create resend nats producer", zap.Error(err))
		return
	}

	fsResend := nats.NewFSResend(
		config.Directory,
		resendProducer,
		nats.WithFSResendLogger(logger),
	)
	defer fsResend.Close() //nolint:errcheck

	if err := fsResend.Resend(); err != nil {
		logger.Error("failed to run resend", zap.Error(err))
		return
	}
}

type appConfig struct {
	Directory string     `long:"dir"       description:"Directory with fallback messages" required:"true"`
	NATS      configNATS `namespace:"nats" group:"NATS configuration"`
}

type configNATS struct {
	URLs []string `default:"nats://localhost:4222" long:"urls" description:"List of NATS servers"`
}
