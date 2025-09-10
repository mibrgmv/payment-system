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
	if err := s.validateDepositInput(toAccountID, amount, idempotencyKey); err != nil {
		return nil, err
	}

	existingTransaction, err := s.repo.GetTransactionByIdempotencyKey(ctx, idempotencyKey)
	if err != nil && !errors.Is(err, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("failed to check idempotency key: %w", err)
	}
	if existingTransaction != nil {
		if s.isIdempotentRequest(existingTransaction, toAccountID, amount, currency, models.TransactionTypeDeposit) {
			return existingTransaction, nil
		}
		return nil, fmt.Errorf("%w: different transaction exists with key %s", ErrIdempotencyConflict, idempotencyKey)
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
		if errors.Is(err, ErrIdempotencyConflict) {
			if existingTx, getErr := s.repo.GetTransactionByIdempotencyKey(ctx, idempotencyKey); getErr == nil {
				if s.isIdempotentRequest(existingTx, toAccountID, amount, currency, models.TransactionTypeDeposit) {
					return existingTx, nil
				}
			}
			return nil, fmt.Errorf("%w: %s", ErrIdempotencyConflict, idempotencyKey)
		}
		return nil, fmt.Errorf("failed to create deposit: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) validateDepositInput(toAccountID string, amount float64, idempotencyKey string) error {
	if toAccountID == "" {
		return fmt.Errorf("%w: to_account_id is required", ErrInvalidTransaction)
	}
	if _, err := uuid.Parse(toAccountID); err != nil {
		return fmt.Errorf("%w: to_account_id must be a valid UUID: %s", ErrInvalidTransaction, toAccountID)
	}
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be positive, got %f", ErrInvalidTransaction, amount)
	}
	if idempotencyKey == "" {
		return fmt.Errorf("%w: idempotency_key is required", ErrInvalidTransaction)
	}
	return nil
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

func (s *transactionService) isIdempotentRequest(existingTransaction *models.Transaction, toAccountID string, amount float64, currency models.Currency, txType models.TransactionType) bool {
	return existingTransaction.Type == txType &&
		existingTransaction.ToAccountID != nil &&
		*existingTransaction.ToAccountID == toAccountID &&
		existingTransaction.Amount == amount &&
		existingTransaction.Currency == currency
}
