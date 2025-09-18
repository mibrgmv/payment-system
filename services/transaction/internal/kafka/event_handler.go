package kafka

import (
	"context"
	"fmt"

	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/shared/json"
	"github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
)

type TransactionEventHandler struct{}

func NewTransactionEventHandler() *TransactionEventHandler {
	return &TransactionEventHandler{}
}

func (h *TransactionEventHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	switch event.EventType {
	case "transaction_created":
		var payload events.TransactionCreated
		if err := json.StrictUnmarshal(event.RawPayload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal transaction created event: %w", err)
		}
		return producer.Produce(ctx, event.Topic, event.EventID, payload)

	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}
