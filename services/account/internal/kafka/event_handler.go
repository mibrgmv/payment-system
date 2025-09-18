package kafka

import (
	"context"
	"fmt"

	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/shared/json"
	"github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
)

type AccountEventHandler struct{}

func NewAccountEventHandler() *AccountEventHandler {
	return &AccountEventHandler{}
}

func (h *AccountEventHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	switch event.EventType {
	case "balance_updated":
		var payload events.BalanceUpdated
		if err := json.StrictUnmarshal(event.RawPayload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal balance updated event: %w", err)
		}
		return producer.Produce(ctx, event.Topic, event.EventID, payload)

	case "transaction_result":
		var payload events.TransactionResult
		if err := json.StrictUnmarshal(event.RawPayload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal transaction result event: %w", err)
		}
		return producer.Produce(ctx, event.Topic, event.EventID, payload)

	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}
