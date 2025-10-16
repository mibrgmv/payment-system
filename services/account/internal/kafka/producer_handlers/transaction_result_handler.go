package producer_handlers

import (
	"context"
	"fmt"

	"github.com/mibrgmv/payment-system/account/internal/kafka/events"
	"github.com/mibrgmv/payment-system/shared/json"
	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/outbox"
)

type TransactionResultHandler struct{}

func NewTransactionResultHandler() *TransactionResultHandler {
	return &TransactionResultHandler{}
}

func (h *TransactionResultHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	var payload events.TransactionResult
	if err := json.StrictUnmarshal(event.RawPayload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal transaction result event: %w", err)
	}
	return producer.Produce(ctx, event.Topic, event.EventID, payload)
}

func (h *TransactionResultHandler) GetEventType() string {
	return "transaction_result"
}
