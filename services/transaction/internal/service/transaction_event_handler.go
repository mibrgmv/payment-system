package service

import (
	"context"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

type TransactionEventHandler interface {
	HandleTransactionResult(ctx context.Context, tx pgx.Tx, event events.TransactionResult) error
}

type transactionEventHandler struct {
	transactionRepo repository.TransactionRepository
	outboxRepo      repository.OutboxRepository
}

func NewTransactionEventHandler(
	transactionRepo repository.TransactionRepository,
	outboxRepo repository.OutboxRepository,
) TransactionEventHandler {
	return &transactionEventHandler{
		transactionRepo: transactionRepo,
		outboxRepo:      outboxRepo,
	}
}

func (h *transactionEventHandler) HandleTransactionResult(ctx context.Context, tx pgx.Tx, event events.TransactionResult) error {
	var status models.TransactionStatus
	var errorMsg *string

	if event.Status == "completed" {
		status = models.TransactionStatusCompleted
	} else {
		status = models.TransactionStatusFailed
		errorMsg = &event.FailureReason
	}

	err := h.transactionRepo.UpdateTransactionStatusTx(ctx, tx, event.TransactionID, status, errorMsg)
	if errors.Is(err, repository.ErrTransactionNotFound) {
		log.Printf("transaction %s not found for event %s, skipping", event.TransactionID, event.EventID)
		return nil
	}

	return err
}
