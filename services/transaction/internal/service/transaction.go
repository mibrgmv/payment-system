package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

var (
	ErrInvalidTransaction   = errors.New("invalid transaction")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrIdempotencyConflict  = errors.New("idempotency key conflict")
	ErrTransactionNotActive = errors.New("transaction is not in active state")
	ErrTransactionNotFound  = repository.ErrTransactionNotFound
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
	if fromAccountID == toAccountID {
		return nil, fmt.Errorf("%w: cannot transfer to same account", ErrInvalidTransaction)
	}

	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.repo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
	if err != nil && !errors.Is(err, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("failed to check idempotency: %w", err)
	}
	if existing != nil {
		if !s.isIdempotentTransferRequest(existing, fromAccountID, toAccountID, amount, currency) {
			return nil, fmt.Errorf("%w: different transaction exists with key %s", ErrIdempotencyConflict, idempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return existing, nil
	}

	transaction := &models.Transaction{
		Type:           models.TransactionTypeTransfer,
		FromAccountID:  &fromAccountID,
		ToAccountID:    &toAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
	}

	if err := s.repo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create transfer: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.repo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
	if err != nil && !errors.Is(err, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("failed to check idempotency: %w", err)
	}
	if existing != nil {
		if !s.isIdempotentDepositRequest(existing, toAccountID, amount, currency) {
			return nil, fmt.Errorf("%w: different transaction exists with key %s", ErrIdempotencyConflict, idempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return existing, nil
	}

	transaction := &models.Transaction{
		Type:           models.TransactionTypeDeposit,
		ToAccountID:    &toAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
	}

	if err := s.repo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create deposit: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
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
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.repo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
	if err != nil && !errors.Is(err, repository.ErrTransactionNotFound) {
		return nil, fmt.Errorf("failed to check idempotency: %w", err)
	}
	if existing != nil {
		if !s.isIdempotentWithdrawalRequest(existing, fromAccountID, amount, currency) {
			return nil, fmt.Errorf("%w: different transaction exists with key %s", ErrIdempotencyConflict, idempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return existing, nil
	}

	transaction := &models.Transaction{
		Type:           models.TransactionTypeWithdrawal,
		FromAccountID:  &fromAccountID,
		Amount:         amount,
		Currency:       currency,
		Status:         models.TransactionStatusPending,
		IdempotencyKey: idempotencyKey,
	}

	if err := s.repo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create withdrawal: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error) {
	return s.repo.ListTransactions(ctx, filters, pageSize, pageToken)
}

func (s *transactionService) CancelTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	if err := s.repo.UpdateTransactionStatus(ctx, transactionID, models.TransactionStatusCancelled, nil); err != nil {
		return nil, err
	}
	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) GetTransactionStatus(ctx context.Context, transactionID string) (*models.Transaction, error) {
	return s.repo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) isIdempotentDepositRequest(existingTransaction *models.Transaction, toAccountID string, amount float64, currency models.Currency) bool {
	return existingTransaction.Type == models.TransactionTypeDeposit &&
		existingTransaction.ToAccountID != nil &&
		*existingTransaction.ToAccountID == toAccountID &&
		existingTransaction.Amount == amount &&
		existingTransaction.Currency == currency
}

func (s *transactionService) isIdempotentWithdrawalRequest(existingTransaction *models.Transaction, fromAccountID string, amount float64, currency models.Currency) bool {
	return existingTransaction.Type == models.TransactionTypeWithdrawal &&
		existingTransaction.FromAccountID != nil &&
		*existingTransaction.FromAccountID == fromAccountID &&
		existingTransaction.Amount == amount &&
		existingTransaction.Currency == currency
}

func (s *transactionService) isIdempotentTransferRequest(existingTransaction *models.Transaction, fromAccountID, toAccountID string, amount float64, currency models.Currency) bool {
	return existingTransaction.Type == models.TransactionTypeTransfer &&
		existingTransaction.FromAccountID != nil &&
		*existingTransaction.FromAccountID == fromAccountID &&
		existingTransaction.ToAccountID != nil &&
		*existingTransaction.ToAccountID == toAccountID &&
		existingTransaction.Amount == amount &&
		existingTransaction.Currency == currency
}
