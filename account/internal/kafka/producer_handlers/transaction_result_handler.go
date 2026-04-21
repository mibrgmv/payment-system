package producer_handlers

import (
	"context"

	"github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/outbox"
)

type TransactionResultHandler struct{}

func NewTransactionResultHandler() *TransactionResultHandler {
	return &TransactionResultHandler{}
}

func (h *TransactionResultHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	return producer.Produce(ctx, event.Topic, event.EventID, event.RawPayload)
}

func (h *TransactionResultHandler) GetEventType() string {
	return "transaction_result"
}
