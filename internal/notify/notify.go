package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	queueURL string
}

func NewConsumer(ctx context.Context, queueURL string, log *slog.Logger) (*Consumer, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &Consumer{
		client:   sqs.NewFromConfig(cfg),
		queueURL: queueURL,
		log:      log,
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
			// all good
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

		for _, msg := range output.Messages {
			c.processMessage(ctx, msg)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg types.Message) {
	var event CDCEvent
	if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &event); err != nil {
		c.log.Error("failed to parse cdc event", "error", err, "body", aws.ToString(msg.Body))
		c.deleteMessage(ctx, msg.ReceiptHandle)
		return
	}

	c.logEvent(event)
	c.deleteMessage(ctx, msg.ReceiptHandle)
}

func (c *Consumer) logEvent(event CDCEvent) {
	switch event.Op {
	case "c":
		c.log.Info("product created",
			"product_id", event.After.ID,
			"name", event.After.Name,
			"price", event.After.Price,
		)

	case "u":
		c.log.Info("product deleted",
			"product_id", event.After.ID,
			"name", event.After.Name,
		)

	case "r", "d":
		//skip

	default:
		c.log.Warn("unknown cdc operation", "op", event.Op)
	}
}

func (c *Consumer) deleteMessage(ctx context.Context, receiptHandle *string) {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		c.log.Error("failed to delete message from queue", "error", err)
	}
}
