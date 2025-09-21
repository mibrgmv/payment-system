package consumer_handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	sharedevents "github.com/mibrgmv/payment-service/shared/events"
	sharedjson "github.com/mibrgmv/payment-service/shared/json"
)

type TransactionCreatedHandler struct {
	transactionService service.TransactionService
}

func NewTransactionCreatedHandler(transactionService service.TransactionService) sharedevents.ProcessorEventHandler {
	return &TransactionCreatedHandler{
		transactionService: transactionService,
	}
}

func (h *TransactionCreatedHandler) HandleEvent(ctx context.Context, tx pgx.Tx, eventData []byte) error {
	var event events.TransactionCreated
	if err := sharedjson.StrictUnmarshal(eventData, &event); err != nil {
		return fmt.Errorf("failed to unmarshal transaction created: %w", err)
	}
	return h.transactionService.HandleTransactionCreated(ctx, tx, event)
}

func (h *TransactionCreatedHandler) GetEventType() string {
	return "transaction_created"
}

func (h *TransactionCreatedHandler) GetEventID(eventData []byte) (string, error) {
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(eventData, &event); err != nil {
		return "", fmt.Errorf("failed to extract event ID: %w", err)
	}
	return event.EventID, nil
}
