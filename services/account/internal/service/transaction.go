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
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
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
	if err := s.validateInput(ctx, tx, event); err != nil {
		if publishErr := s.publishTransactionResult(ctx, tx, event.TransactionID, err); publishErr != nil {
			return fmt.Errorf("failed to publish transaction result: %w", publishErr)
		}
		if errors.Is(err, ErrCurrencyMismatch) || errors.Is(err, ErrInsufficientFunds) {
			return nil
		}
		return fmt.Errorf("failed to validate transaction created: %w", err)
	}

	var processingErr error
	switch event.Type {
	case "transfer":
		processingErr = s.processTransfer(ctx, tx, event)
	case "deposit":
		processingErr = s.processDeposit(ctx, tx, event)
	case "withdrawal":
		processingErr = s.processWithdrawal(ctx, tx, event)
	default:
		processingErr = fmt.Errorf("unknown transaction type: %s", event.Type)
	}

	if err := s.publishTransactionResult(ctx, tx, event.TransactionID, processingErr); err != nil {
		return fmt.Errorf("failed to publish transaction result: %w", err)
	}

	return processingErr
}

func (s *transactionService) validateInput(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error {
	if event.FromAccountID != "" {
		exists, err := s.accountRepo.AccountExistsTx(ctx, tx, event.FromAccountID)
		if err != nil {
			return fmt.Errorf("failed to check from account: %w", err)
		}
		if !exists {
			return fmt.Errorf("from account %s does not exist", event.FromAccountID)
		}
	}

	if event.ToAccountID != "" {
		exists, err := s.accountRepo.AccountExistsTx(ctx, tx, event.ToAccountID)
		if err != nil {
			return fmt.Errorf("failed to check to account: %w", err)
		}
		if !exists {
			return fmt.Errorf("to account %s does not exist", event.ToAccountID)
		}
	}

	if event.Currency != "" {
		account, err := s.accountRepo.GetAccountTx(ctx, tx, event.ToAccountID)
		if err != nil {
			return fmt.Errorf("failed to get to account details: %w", err)
		}
		if account.Currency.String() != event.Currency {
			return ErrCurrencyMismatch
		}
	}

	return nil
}

func (s *transactionService) processTransfer(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error {
	if _, err := s.balanceRepo.UpdateBalanceTx(ctx, tx, event.FromAccountID, -event.Amount); err != nil {
		return fmt.Errorf("failed to transfer from account: %w", err)
	}

	if _, err := s.balanceRepo.UpdateBalanceTx(ctx, tx, event.ToAccountID, event.Amount); err != nil {
		return fmt.Errorf("failed to transfer to account: %w", err)
	}

	return nil
}

func (s *transactionService) processDeposit(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error {
	if _, err := s.balanceRepo.UpdateBalanceTx(ctx, tx, event.ToAccountID, event.Amount); err != nil {
		return fmt.Errorf("failed to deposit to account: %w", err)
	}

	return nil
}

func (s *transactionService) processWithdrawal(ctx context.Context, tx pgx.Tx, event events.TransactionCreated) error {
	balance, err := s.balanceRepo.GetBalanceTx(ctx, tx, event.FromAccountID)
	if err != nil {
		return fmt.Errorf("failed to get balance: %w", err)
	}

	if balance.Amount < event.Amount {
		return fmt.Errorf("insufficient funds: available %.2f, requested %.2f", balance.Amount, event.Amount)
	}

	if _, err := s.balanceRepo.UpdateBalanceTx(ctx, tx, event.FromAccountID, -event.Amount); err != nil {
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
