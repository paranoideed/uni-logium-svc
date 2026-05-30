package app

import (
	"context"
	"sync"

	"github.com/paranoideed/uni-logium-svc/internal/notify"
)

func (a *App) Run(ctx context.Context) error {
	log := a.Logger()

	consumer, err := notify.NewConsumer(ctx, a.config.SQS.QueueURL, log)
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
