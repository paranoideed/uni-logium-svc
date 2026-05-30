package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	statusSuccess = attribute.String("status", "success")
	statusError   = attribute.String("status", "error")
)

type Metrics struct {
	processed metric.Int64Counter
	received  metric.Int64Counter
	duration  metric.Float64Histogram
}

func New() (*Metrics, error) {
	meter := otel.Meter("uni-logium-svc")

	processed, err := meter.Int64Counter("notifications_processed_total",
		metric.WithDescription("Total number of processed SQS notifications"),
	)
	if err != nil {
		return nil, err
	}

	received, err := meter.Int64Counter("sqs_messages_received_total",
		metric.WithDescription("Total number of messages received from SQS"),
	)
	if err != nil {
		return nil, err
	}

	duration, err := meter.Float64Histogram("notification_processing_duration_seconds",
		metric.WithDescription("Time spent processing a single notification"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		processed: processed,
		received:  received,
		duration:  duration,
	}, nil
}

func (m *Metrics) RecordReceived(ctx context.Context, count int) {
	m.received.Add(ctx, int64(count))
}

func (m *Metrics) RecordProcessed(ctx context.Context, start time.Time, err error) {
	elapsed := time.Since(start).Seconds()
	m.duration.Record(ctx, elapsed)

	if err != nil {
		m.processed.Add(ctx, 1, metric.WithAttributes(statusError))
		return
	}
	m.processed.Add(ctx, 1, metric.WithAttributes(statusSuccess))
}
