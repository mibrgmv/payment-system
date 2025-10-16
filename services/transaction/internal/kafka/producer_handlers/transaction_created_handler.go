package producer_handlers

import (
	"context"
	"fmt"

	"github.com/mibrgmv/payment-system/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-system/shared/json"
	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/outbox"
)

type TransactionCreatedHandler struct{}

func NewTransactionCreatedHandler() *TransactionCreatedHandler {
	return &TransactionCreatedHandler{}
}

func (h *TransactionCreatedHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	var payload events.TransactionCreated
	if err := json.StrictUnmarshal(event.RawPayload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal transaction created event: %w", err)
	}
	return producer.Produce(ctx, event.Topic, event.EventID, payload)
}

func (h *TransactionCreatedHandler) GetEventType() string {
	return "transaction_created"
}
