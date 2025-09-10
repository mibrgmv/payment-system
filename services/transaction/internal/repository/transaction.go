package repository

import (
	"context"
	"errors"

	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

var (
	ErrTransactionNotFound = errors.New("transaction not found")
)

type TransactionRepository interface {
	CreateTransaction(ctx context.Context, transaction *models.Transaction) error
	GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error)
	ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error)
	UpdateTransactionStatus(ctx context.Context, transactionID string, status models.TransactionStatus, errorMessage *string) error
	CancelTransaction(ctx context.Context, transactionID string) error
}
