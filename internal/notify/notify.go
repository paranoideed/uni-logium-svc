package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/paranoideed/uni-logium-svc/internal/metrics"
)

//go:generate mockery --name=sqsClient --inpackage --filename=mock_sqs_client_test.go
type sqsClient interface {
	ReceiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

//go:generate mockery --name=metricsRecorder --inpackage --filename=mock_metrics_recorder_test.go
type metricsRecorder interface {
	RecordReceived(ctx context.Context, count int)
	RecordProcessed(ctx context.Context, start time.Time, err error)
}

type Consumer struct {
	client            sqsClient
	log               *slog.Logger
	metrics           metricsRecorder
	queueURL          string
	workers           int
	fetchers          int
	visibilityTimeout time.Duration
}

func newConsumer(
	client sqsClient,
	log *slog.Logger,
	m metricsRecorder,
	queueURL string,
	workers int,
	fetchers int,
	visibilityTimeout time.Duration,
) *Consumer {
	if workers <= 0 {
		workers = 1
	}
	if fetchers <= 0 {
		fetchers = 1
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}

	return &Consumer{
		client:            client,
		log:               log,
		metrics:           m,
		queueURL:          queueURL,
		workers:           workers,
		fetchers:          fetchers,
		visibilityTimeout: visibilityTimeout,
	}
}

func NewConsumer(
	ctx context.Context,
	log *slog.Logger,
	m *metrics.Metrics,
	queueURL string,
	workers int,
	fetchers int,
	visibilityTimeout time.Duration,
) (*Consumer, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return newConsumer(sqs.NewFromConfig(cfg), log, m, queueURL, workers, fetchers, visibilityTimeout), nil
}

type sqsMessage struct {
	msg        types.Message
	receivedAt time.Time
}

func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("starting sqs consumer", "queue", c.queueURL, "workers", c.workers, "fetchers", c.fetchers)

	msgCh := make(chan sqsMessage, (c.fetchers+c.workers)*10)

	var wg sync.WaitGroup

	var fetchWg sync.WaitGroup
	for range c.fetchers {
		fetchWg.Go(func() {
			c.fetch(ctx, msgCh)
		})
	}

	wg.Go(func() {
		fetchWg.Wait()
		close(msgCh)
	})

	for range c.workers {
		wg.Go(func() {
			for m := range msgCh {
				elapsed := time.Since(m.receivedAt)
				remaining := c.visibilityTimeout - elapsed
				if remaining <= 0 {
					c.log.Warn("message visibility timeout likely expired, skipping",
						"msg_id", aws.ToString(m.msg.MessageId),
						"elapsed", elapsed,
					)
					continue
				}

				msgCtx, cancel := context.WithTimeout(ctx, remaining)
				c.handleMessage(msgCtx, m.msg)
				cancel()
			}
		})
	}

	wg.Wait()
	c.log.Info("stopping sqs consumer", "queue", c.queueURL)
}

func (c *Consumer) fetch(ctx context.Context, msgCh chan<- sqsMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		output, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     10,
		})
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			c.log.Error("failed to receive message", "error", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		if len(output.Messages) > 0 {
			c.metrics.RecordReceived(ctx, len(output.Messages))
		}

		now := time.Now()
		for _, msg := range output.Messages {
			select {
			case msgCh <- sqsMessage{msg: msg, receivedAt: now}:
			case <-ctx.Done():
				return
			}
		}
	}
}

type CDCEvent struct {
	Before *ProductPayload `json:"before"`
	After  *ProductPayload `json:"after"`
	Op     string          `json:"op"`
	TsMs   int64           `json:"ts_ms"`
}

type ProductPayload struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	CreatedAt any     `json:"created_at"`
	DeletedAt any     `json:"deleted_at"`
}

func (c *Consumer) handleMessage(ctx context.Context, msg types.Message) {
	start := time.Now()

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			c.log.Error("panic recovered in message handler", "panic", r, "msg_id", aws.ToString(msg.MessageId))
		}

		c.metrics.RecordProcessed(ctx, start, err)
	}()

	var event CDCEvent
	if err = json.Unmarshal([]byte(aws.ToString(msg.Body)), &event); err != nil {
		c.log.Error("failed to parse cdc event", "error", err)
		//I think better not to delete the message anyway it will eventually be moved to the DLQ
		return
	}

	err = c.logMessage(event)
	if err != nil {
		return
	}

	_, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		c.log.Error("failed to delete message from queue", "error", err)
	}
}

func (c *Consumer) logMessage(event CDCEvent) error {
	if rand.IntN(10) == 0 {
		productID := ""
		switch {
		case event.Before != nil && event.Before.ID != "":
			productID = event.Before.ID
		case event.After != nil && event.After.ID != "":
			productID = event.After.ID
		}
		c.log.Error("simulated transient error for testing retry mechanism", "product_id", productID)
		return fmt.Errorf("simulated transient error")
	}

	switch event.Op {
	case "c":
		if event.After == nil {
			c.log.Warn("missing after payload for create event")
			return nil
		}
		c.log.Info("product created",
			"product_id", event.After.ID,
			"name", event.After.Name,
			"price", event.After.Price,
			"created_at", event.After.CreatedAt,
		)

	case "u":
		if event.After == nil {
			c.log.Warn("missing after payload for update event")
			return nil
		}
		c.log.Info("product deleted",
			"product_id", event.After.ID,
			"name", event.After.Name,
			"price", event.After.Price,
			"deleted_at", event.After.DeletedAt,
		)

	case "r", "d":
		// skip

	default:
		c.log.Warn("unknown cdc operation", "op", event.Op)
	}

	return nil
}
