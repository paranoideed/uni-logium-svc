package app

import (
	"context"
	"sync"

	"github.com/paranoideed/uni-logium-svc/internal/metrics"
	"github.com/paranoideed/uni-logium-svc/internal/notify"
	"github.com/paranoideed/uni-logium-svc/internal/rest"
	"github.com/paranoideed/uni-logium-svc/internal/telemetry"
)

func (a *App) Run(ctx context.Context) error {
	shutdown, err := telemetry.Setup(ctx, "uni-logium-svc")
	if err != nil {
		return err
	}

	log := a.Logger()

	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Error("failed to shutdown telemetry gracefully", "error", err)
		}
	}()

	m, err := metrics.New()
	if err != nil {
		return err
	}

	consumer, err := notify.NewConsumer(ctx, log, m, a.config.SQS.QueueURL, a.config.SQS.Workers, a.config.SQS.Fetchers, a.config.SQS.VisibilityTimeout)
	if err != nil {
		return err
	}

	log.Info("application started", "sqs_workers", a.config.SQS.Workers, "sqs_fetchers", a.config.SQS.Fetchers)

	server := rest.NewServer(func(ctx context.Context) error { return nil })

	var wg sync.WaitGroup
	wg.Go(func() { consumer.Run(ctx) })
	wg.Go(func() {
		server.Run(ctx, log, rest.Config{
			Port:         a.config.Rest.Port,
			ReadTimeout:  a.config.Rest.ReadTimeout,
			WriteTimeout: a.config.Rest.WriteTimeout,
			IdleTimeout:  a.config.Rest.IdleTimeout,
		})
	})
	wg.Wait()

	return nil
}
