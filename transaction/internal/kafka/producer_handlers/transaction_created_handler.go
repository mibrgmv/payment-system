package producer_handlers

import (
	"context"

	"github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/outbox"
)

type TransactionCreatedHandler struct{}

func NewTransactionCreatedHandler() *TransactionCreatedHandler {
	return &TransactionCreatedHandler{}
}

func (h *TransactionCreatedHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	return producer.Produce(ctx, event.Topic, event.EventID, event.RawPayload)
}

func (h *TransactionCreatedHandler) GetEventType() string {
	return "transaction_created"
}
