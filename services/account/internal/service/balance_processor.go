package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type BalanceProcessor struct {
	balanceRepo       repository.BalanceRepository
	eventTrackingRepo repository.EventTrackingRepository
	pool              *pgxpool.Pool
}

func NewBalanceProcessor(
	balanceRepo repository.BalanceRepository,
	eventTrackingRepo repository.EventTrackingRepository,
	pool *pgxpool.Pool,
) *BalanceProcessor {
	return &BalanceProcessor{
		balanceRepo:       balanceRepo,
		eventTrackingRepo: eventTrackingRepo,
		pool:              pool,
	}
}

func (bp *BalanceProcessor) ProcessBalanceChangeEvent(ctx context.Context, messageValue []byte) error {
	var event models.BalanceChangeEvent
	if err := json.Unmarshal(messageValue, &event); err != nil {
		return fmt.Errorf("failed to unmarshal balance change event: %w", err)
	}

	processed, err := bp.eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("failed to check if event is processed: %w", err)
	}
	if processed {
		log.Printf("Event %s already processed, skipping", event.EventID)
		return nil
	}

	tx, err := bp.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	err = bp.eventTrackingRepo.MarkEventProcessed(ctx, tx, event.EventID, event.AccountID)
	if err != nil {
		if errors.Is(err, repository.ErrEventAlreadyProcessed) {
			log.Printf("Event %s already processed concurrently, skipping", event.EventID)
			return nil
		}
		return fmt.Errorf("failed to mark event as processed: %w", err)
	}

	balance, err := bp.balanceRepo.UpdateBalanceTx(ctx, tx, event.AccountID, event.Amount)
	if err != nil {
		if errors.Is(err, repository.ErrBalanceNotFound) {
			log.Printf("Account %s not found for event %s, skipping", event.AccountID, event.EventID)
		} else {
			return fmt.Errorf("failed to update balance: %w", err)
		}
	} else {
		log.Printf("Updated balance for account %s: new amount = %f", balance.AccountID, balance.Amount)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
