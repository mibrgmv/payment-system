package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
)

type eventTrackingRepo struct {
	pool *pgxpool.Pool
}

func NewEventTrackingRepository(pool *pgxpool.Pool) repository.EventTrackingRepository {
	return &eventTrackingRepo{pool: pool}
}

func (r *eventTrackingRepo) MarkEventProcessedTx(ctx context.Context, tx pgx.Tx, eventID, eventType string) error {
	sql := `
	insert into processed_events (event_id, event_type, source_service)
	values ($1, $2, $3)
	on conflict (event_id) do nothing
    `

	result, err := tx.Exec(ctx, sql, eventID, eventType, "transaction_service")
	if err != nil {
		return fmt.Errorf("failed to mark event as processed: %w", err)
	}
	if result.RowsAffected() == 0 {
		return repository.ErrEventAlreadyProcessed
	}

	return nil
}

func (r *eventTrackingRepo) IsEventProcessed(ctx context.Context, eventID string) (bool, error) {
	sql := `
	select exists(select 1 from processed_events where event_id = $1)
    `

	var exists bool
	err := r.pool.QueryRow(ctx, sql, eventID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if event is processed: %w", err)
	}

	return exists, nil
}

func (r *eventTrackingRepo) CleanupOldEvents(ctx context.Context, olderThanDays int) error {
	sql := `delete from processed_events where created_at < now() - interval '1 day' * $1`

	_, err := r.pool.Exec(ctx, sql, olderThanDays)
	if err != nil {
		return fmt.Errorf("failed to cleanup old events: %w", err)
	}

	return nil
}
