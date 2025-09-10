package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

var (
	ErrTransactionIDRequired = errors.New("transaction ID is required")
	ErrInvalidTransaction    = errors.New("invalid transaction")
	ErrInvalidTransactionID  = errors.New("invalid transaction ID format")
	ErrInsufficientFunds     = errors.New("insufficient funds")
	ErrIdempotencyConflict   = errors.New("idempotency key conflict")
	ErrTransactionNotActive  = errors.New("transaction is not in active state")
	ErrTransactionNotFound   = repository.ErrTransactionNotFound
)

type TransactionService interface {
	CreateTransfer(ctx context.Context, fromAccountID, toAccountID string, amount float64, currency models.Currency, idempotencyKey string) (*models.Transaction, error)
	CreateDeposit(ctx context.Context, toAccountID string, amount float64, currency models.Currency, idempotencyKey string) (*models.Transaction, error)
	CreateWithdrawal(ctx context.Context, fromAccountID string, amount float64, currency models.Currency, idempotencyKey string) (*models.Transaction, error)
	GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error)
	ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error)
	CancelTransaction(ctx context.Context, transactionID string) (*models.Transaction, error)
	GetTransactionStatus(ctx context.Context, transactionID string) (*models.Transaction, error)
}

type transactionService struct {
	repo repository.TransactionRepository
}

func NewTransactionService(repo repository.TransactionRepository) TransactionService {
	return &transactionService{repo: repo}
}

func (s *transactionService) CreateTransfer(
	ctx context.Context,
	fromAccountID, toAccountID string,
	amount float64,
	currency models.Currency,
	idempotencyKey string,
) (*models.Transaction, error) {
	if fromAccountID == "" || toAccountID == "" {
		return nil, fmt.Errorf("%w: both accounts must be specified", ErrInvalidTransaction)
	}
	if fromAccountID == toAccountID {
		return nil, fmt.Errorf("%w: cannot transfer to same account", ErrInvalidTransaction)
	}
	if amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidTransaction)
	}

	transaction := &models.Transaction{
		TransactionID:  uuid.New().String(),
		Type:           models.TransactionTypeTransfer,
		FromAccountID:  &fromAccountID,
		ToAccountID:    &toAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create transfer: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) CreateDeposit(
	ctx context.Context,
	toAccountID string,
	amount float64,
	currency models.Currency,
	idempotencyKey string,
) (*models.Transaction, error) {
	if toAccountID == "" {
		return nil, fmt.Errorf("%w: to account must be specified", ErrInvalidTransaction)
	}
	if amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidTransaction)
	}

	transaction := &models.Transaction{
		TransactionID:  uuid.New().String(),
		Type:           models.TransactionTypeDeposit,
		ToAccountID:    &toAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create deposit: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) CreateWithdrawal(
	ctx context.Context,
	fromAccountID string,
	amount float64,
	currency models.Currency,
	idempotencyKey string,
) (*models.Transaction, error) {
	if fromAccountID == "" {
		return nil, fmt.Errorf("%w: from account must be specified", ErrInvalidTransaction)
	}
	if amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidTransaction)
	}

	transaction := &models.Transaction{
		TransactionID:  uuid.New().String(),
		Type:           models.TransactionTypeWithdrawal,
		FromAccountID:  &fromAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := s.repo.CreateTransaction(ctx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create withdrawal: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	if err := s.validateTransactionID(transactionID); err != nil {
		return nil, err
	}

	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error) {
	return s.repo.ListTransactions(ctx, filters, pageSize, pageToken)
}

func (s *transactionService) CancelTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	if err := s.validateTransactionID(transactionID); err != nil {
		return nil, err
	}

	if err := s.repo.CancelTransaction(ctx, transactionID); err != nil {
		return nil, err
	}
	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) GetTransactionStatus(ctx context.Context, transactionID string) (*models.Transaction, error) {
	if err := s.validateTransactionID(transactionID); err != nil {
		return nil, err
	}

	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) validateTransactionID(transactionID string) error {
	if transactionID == "" {
		return ErrTransactionIDRequired
	}

	if _, err := uuid.Parse(transactionID); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidTransactionID, transactionID)
	}

	return nil
}
