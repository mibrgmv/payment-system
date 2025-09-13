package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

var (
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
)

type TransactionRepository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	CreateTransactionTx(ctx context.Context, tx pgx.Tx, transaction *models.Transaction) error
	GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error)
	GetTransactionByIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, idempotencyKey string) (*models.Transaction, error)
	ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error)
	UpdateTransactionStatus(ctx context.Context, transactionID string, status models.TransactionStatus, errorMessage *string) error
	UpdateTransactionStatusTx(ctx context.Context, tx pgx.Tx, transactionID string, status models.TransactionStatus, errorMessage *string) error
}
