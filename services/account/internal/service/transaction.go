package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/shared/outbox"
)

var (
	ErrNonexistentAccountID = errors.New("nonexistent account id")
	ErrInsufficientFunds    = errors.New("insufficient funds")
	ErrCurrencyMismatch     = errors.New("currency mismatch")
)

type TransactionService interface {
	HandleTransactionCreated(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error
}

type transactionService struct {
	balanceRepo repository.BalanceRepository
	accountRepo repository.AccountRepository
	outboxRepo  outbox.Repository
}

func NewTransactionService(
	balanceRepo repository.BalanceRepository,
	accountRepo repository.AccountRepository,
	outboxRepo outbox.Repository,
) TransactionService {
	return &transactionService{
		balanceRepo: balanceRepo,
		accountRepo: accountRepo,
		outboxRepo:  outboxRepo,
	}
}

func (s *transactionService) HandleTransactionCreated(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error {
	var processingErr error
	switch event.Type {
	case "transfer":
		processingErr = s.processBalanceChange(ctx, tx, event.FromAccountID, -event.Amount, event.Currency)
		if processingErr != nil {
			break
		}
		processingErr = s.processBalanceChange(ctx, tx, event.ToAccountID, event.Amount, event.Currency)
	case "deposit":
		processingErr = s.processBalanceChange(ctx, tx, event.ToAccountID, event.Amount, event.Currency)
	case "withdrawal":
		processingErr = s.processBalanceChange(ctx, tx, event.FromAccountID, -event.Amount, event.Currency)
	default:
		processingErr = fmt.Errorf("unknown transaction type: %s", event.Type)
	}

	if processingErr != nil &&
		!(errors.Is(processingErr, ErrNonexistentAccountID) || errors.Is(processingErr, ErrInsufficientFunds) || errors.Is(processingErr, ErrCurrencyMismatch)) {
		return processingErr
	}
	if publishErr := s.publishTransactionResult(ctx, tx, event.TransactionID, processingErr); publishErr != nil {
		return fmt.Errorf("failed to publish transaction result: %w", publishErr)
	}

	return nil
}

func (s *transactionService) processBalanceChange(ctx context.Context, tx pgx.Tx, accountID string, amount float64, currency string) error {
	exists, err := s.accountRepo.AccountExistsTx(ctx, tx, accountID)
	if err != nil {
		return fmt.Errorf("failed to check account existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: %s", ErrNonexistentAccountID, accountID)
	}

	balance, err := s.balanceRepo.GetBalanceTx(ctx, tx, accountID)
	if err != nil {
		return fmt.Errorf("failed to get balance: %w", err)
	}
	if currency != balance.Currency.String() {
		return ErrCurrencyMismatch
	}
	if amount < 0 && balance.Amount < -amount {
		return ErrInsufficientFunds
	}

	if _, err := s.balanceRepo.UpdateBalanceTx(ctx, tx, accountID, amount); err != nil {
		return fmt.Errorf("failed to withdraw from account: %w", err)
	}

	return nil
}

func (s *transactionService) publishTransactionResult(ctx context.Context, tx pgx.Tx, transactionID string, processingErr error) error {
	status := "completed"
	failureReason := ""

	if processingErr != nil {
		status = "failed"
		failureReason = processingErr.Error()
	}

	resultEvent := events.TransactionResult{
		EventID:       uuid.New().String(),
		TransactionID: transactionID,
		Status:        status,
		FailureReason: failureReason,
		Timestamp:     time.Now(),
		EventType:     "transaction_result",
	}

	outboxEvent, err := outbox.NewEvent(resultEvent.EventID, "transaction_result", "transactions.results", resultEvent)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}

	return s.outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
}
