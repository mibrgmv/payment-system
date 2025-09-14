package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
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
	HandleTransactionResult(ctx context.Context, tx pgx.Tx, event events.TransactionResult) error
}

type transactionService struct {
	transactionRepo repository.TransactionRepository
	outboxRepo      repository.OutboxRepository
}

func NewTransactionService(
	transactionRepo repository.TransactionRepository,
	outboxRepo repository.OutboxRepository,
) TransactionService {
	return &transactionService{
		transactionRepo: transactionRepo,
		outboxRepo:      outboxRepo,
	}
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

	tx, err := s.transactionRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.transactionRepo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
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

	if err := s.transactionRepo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create transfer: %w", err)
	}

	if err := s.publishTransactionCreated(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to publish transaction event: %w", err)
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
	tx, err := s.transactionRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.transactionRepo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
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

	if err := s.transactionRepo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create deposit: %w", err)
	}

	if err := s.publishTransactionCreated(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to publish transaction event: %w", err)
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
	tx, err := s.transactionRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.transactionRepo.GetTransactionByIdempotencyKeyTx(ctx, tx, idempotencyKey)
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

	if err := s.transactionRepo.CreateTransactionTx(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to create withdrawal: %w", err)
	}

	if err := s.publishTransactionCreated(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to publish transaction event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return transaction, nil
}

func (s *transactionService) GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	return s.transactionRepo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error) {
	return s.transactionRepo.ListTransactions(ctx, filters, pageSize, pageToken)
}

func (s *transactionService) CancelTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	if err := s.transactionRepo.UpdateTransactionStatus(ctx, transactionID, models.TransactionStatusCancelled, nil); err != nil {
		return nil, err
	}
	return s.transactionRepo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) GetTransactionStatus(ctx context.Context, transactionID string) (*models.Transaction, error) {
	return s.transactionRepo.GetTransaction(ctx, transactionID)
}

func (s *transactionService) HandleTransactionResult(ctx context.Context, tx pgx.Tx, event events.TransactionResult) error {
	var status models.TransactionStatus
	var errorMsg *string

	if event.Status == "completed" {
		status = models.TransactionStatusCompleted
	} else {
		status = models.TransactionStatusFailed
		errorMsg = &event.FailureReason
	}

	return s.transactionRepo.UpdateTransactionStatusTx(ctx, tx, event.TransactionID, status, errorMsg)
}

func (s *transactionService) publishTransactionCreated(ctx context.Context, tx pgx.Tx, transaction *models.Transaction) error {
	var fromAccountID, toAccountID string
	if transaction.FromAccountID != nil {
		fromAccountID = *transaction.FromAccountID
	}
	if transaction.ToAccountID != nil {
		toAccountID = *transaction.ToAccountID
	}

	event := events.TransactionCreated{
		EventID:       uuid.New().String(),
		TransactionID: transaction.TransactionID,
		Type:          string(transaction.Type),
		Amount:        transaction.Amount,
		Currency:      string(transaction.Currency),
		ToAccountID:   toAccountID,
		FromAccountID: fromAccountID,
		Description:   "",
		Timestamp:     time.Now(),
	}

	outboxEvent, err := events.NewOutboxEvent(event.EventID, "transaction_created", "transactions.created", event)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return s.outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
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
