package notify

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/mock"
)

func newTestConsumer(t *testing.T, client sqsClient) *Consumer {
	t.Helper()

	m := new(mockMetricsRecorder)
	m.On("RecordProcessed", mock.Anything, mock.Anything, mock.Anything).Maybe()
	m.On("RecordReceived", mock.Anything, mock.Anything).Maybe()

	return newConsumer(
		client,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		m,
		"https://sqs.test/queue",
		1, 1,
		30*time.Second,
	)
}

func TestHandleMessage_InvalidJSON(t *testing.T) {
	sqsMock := new(mockSqsClient)
	c := newTestConsumer(t, sqsMock)

	msg := types.Message{
		MessageId:     aws.String("msg-1"),
		Body:          aws.String("not valid json {{{"),
		ReceiptHandle: aws.String("receipt-1"),
	}

	c.handleMessage(context.Background(), msg)

	sqsMock.AssertNotCalled(t, "DeleteMessage")
}
