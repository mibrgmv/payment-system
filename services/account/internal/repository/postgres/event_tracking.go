package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
)

type eventTrackingRepo struct {
	pool *pgxpool.Pool
}

func NewEventTrackingRepository(pool *pgxpool.Pool) repository.EventTrackingRepository {
	return &eventTrackingRepo{pool: pool}
}

func (r *eventTrackingRepo) MarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID string, accountID string) error {
	sql := `
	insert into processed_events (event_id, account_id)
	values ($1, $2)
    `

	_, err := tx.Exec(ctx, sql, eventID, accountID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return repository.ErrEventAlreadyProcessed
		}
		return fmt.Errorf("failed to mark event as processed: %w", err)
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
