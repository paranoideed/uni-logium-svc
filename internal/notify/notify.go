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

// CDCEvent is the Debezium Change Data Capture event sent to SQS.
// op values: "c"=create, "u"=update (soft-delete is an UPDATE), "d"=delete, "r"=snapshot read.
type CDCEvent struct {
	Before *ProductPayload `json:"before"`
	After  *ProductPayload `json:"after"`
	Op     string          `json:"op"`
	TsMs   int64           `json:"ts_ms"`
}

// ProductPayload mirrors the products table columns.
// Timestamps from Debezium come as microseconds since epoch (int64) or null,
// so we use interface{} to handle both.
type ProductPayload struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Price     float64     `json:"price"`
	CreatedAt interface{} `json:"created_at"`
	DeletedAt interface{} `json:"deleted_at"`
}

type Consumer struct {
	client   *sqs.Client
	queueURL string
	log      *slog.Logger
}

func NewConsumer(ctx context.Context, queueURL string, log *slog.Logger) (*Consumer, error) {
	// LoadDefaultConfig reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION from env automatically.
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

// Run polls SQS in a loop until ctx is cancelled.
//
// SQS is pull-based: we ask for messages (ReceiveMessage), process them,
// then delete them. If we don't delete, the message re-appears after
// VisibilityTimeout and gets delivered again — at-least-once guarantee.
// WaitTimeSeconds=10 is "long polling" — SQS holds the connection open
// for up to 10s if the queue is empty, which is cheaper than short polling.
func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("starting sqs consumer", "queue", c.queueURL)

	for {
		if ctx.Err() != nil {
			c.log.Info("sqs consumer stopped")
			return
		}

		output, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,  // max per call, hard limit is 10
			WaitTimeSeconds:     10,  // long polling: wait up to 10s for messages
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Error("failed to receive messages", "error", err)
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
		if event.After == nil {
			c.log.Warn("create event missing 'after' payload")
			return
		}
		c.log.Info("product created",
			"product_id", event.After.ID,
			"name", event.After.Name,
			"price", event.After.Price,
		)

	case "u":
		if event.After == nil {
			c.log.Warn("update event missing 'after' payload")
			return
		}
		// Soft delete: UPDATE sets deleted_at = now()
		if event.After.DeletedAt != nil {
			c.log.Info("product deleted",
				"product_id", event.After.ID,
				"name", event.After.Name,
			)
		} else {
			c.log.Info("product updated",
				"product_id", event.After.ID,
				"name", event.After.Name,
			)
		}

	case "r":
		// Snapshot read during initial sync — not a real change, safe to skip.
		c.log.Debug("snapshot event, skipping",
			"product_id", func() string {
				if event.After != nil {
					return event.After.ID
				}
				return ""
			}(),
		)

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
