package consumer_handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	platformevents "github.com/mibrgmv/go-platform/events"
	"github.com/mibrgmv/payment-system/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-system/transaction/internal/service"
)

type TransactionResultHandler struct {
	transactionService service.TransactionService
}

func NewTransactionResultHandler(transactionService service.TransactionService) platformevents.ProcessorEventHandler {
	return &TransactionResultHandler{
		transactionService: transactionService,
	}
}

func (h *TransactionResultHandler) HandleEvent(ctx context.Context, tx pgx.Tx, eventData []byte) error {
	var event events.TransactionResult
	if err := strictUnmarshal(eventData, &event); err != nil {
		return fmt.Errorf("failed to unmarshal transaction result: %w", err)
	}
	return h.transactionService.HandleTransactionResult(ctx, tx, event)
}

func (h *TransactionResultHandler) GetEventType() string {
	return "transaction_result"
}

func (h *TransactionResultHandler) GetEventID(eventData []byte) (string, error) {
	var event struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(eventData, &event); err != nil {
		return "", fmt.Errorf("failed to extract event ID: %w", err)
	}
	return event.EventID, nil
}
