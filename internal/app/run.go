package app

import (
	"context"
	"sync"

	"github.com/paranoideed/uni-logium-svc/internal/metrics"
	"github.com/paranoideed/uni-logium-svc/internal/notify"
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

	consumer, err := notify.NewConsumer(ctx, a.config.SQS.QueueURL, log, m)
	if err != nil {
		return err
	}

	workers := a.config.SQS.Workers
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Run(ctx)
		}()
	}

	log.Info("application started", "sqs_workers", workers)
	wg.Wait()

	return nil
}
