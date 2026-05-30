package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/paranoideed/uni-logium-svc/internal/metrics"
)

type CDCEvent struct {
	Before *ProductPayload `json:"before"`
	After  *ProductPayload `json:"after"`
	Op     string          `json:"op"`
	TsMs   int64           `json:"ts_ms"`
}

type ProductPayload struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Price     float64     `json:"price"`
	CreatedAt interface{} `json:"created_at"`
	DeletedAt interface{} `json:"deleted_at"`
}

type Consumer struct {
	client   *sqs.Client
	log      *slog.Logger
	metrics  *metrics.Metrics
	queueURL string
}

func NewConsumer(ctx context.Context, queueURL string, log *slog.Logger, m *metrics.Metrics) (*Consumer, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &Consumer{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
		log:      log,
		metrics:  m,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("starting sqs consumer", "queue", c.queueURL)

	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopping sqs consumer", "queue", c.queueURL)
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
			continue
		}

		if len(output.Messages) > 0 {
			c.metrics.RecordReceived(ctx, len(output.Messages))
		}

		for _, msg := range output.Messages {
			msgCtx, cancel := context.WithTimeout(ctx, 29*time.Second)
			c.processMessage(msgCtx, msg)
			cancel()
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg types.Message) {
	start := time.Now()

	var err error
	defer func() { c.metrics.RecordProcessed(ctx, start, err) }()

	var event CDCEvent
	if err = json.Unmarshal([]byte(aws.ToString(msg.Body)), &event); err != nil {
		c.log.Error("failed to parse cdc event", "error", err, "body", aws.ToString(msg.Body))
		//I think better not to delete the message anyway it will eventually be moved to the DLQ
		return
	}

	err = c.handleEvent(event)
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

func (c *Consumer) handleEvent(event CDCEvent) error {
	if rand.IntN(10) == 0 {
		c.log.Error("simulated transient error for testing retry mechanism", "product_id", event.After.ID)
		return fmt.Errorf("simulated transient error")
	}

	switch event.Op {
	case "c":
		c.log.Info("product created",
			"product_id", event.After.ID,
			"name", event.After.Name,
			"price", event.After.Price,
			"created_at", event.After.CreatedAt,
		)

	case "u":
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
