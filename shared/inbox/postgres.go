package inbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) Repository {
	return &postgresRepo{pool: pool}
}

func (r *postgresRepo) MarkEventProcessedTx(ctx context.Context, tx pgx.Tx, eventID, eventType string) error {
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
		return ErrEventAlreadyProcessed
	}

	return nil
}

func (r *postgresRepo) IsEventProcessed(ctx context.Context, eventID string) (bool, error) {
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

func (r *postgresRepo) CleanupOldEvents(ctx context.Context, olderThanDays int) error {
	sql := `delete from processed_events where processed_at < now() - interval '1 day' * $1`

	_, err := r.pool.Exec(ctx, sql, olderThanDays)
	if err != nil {
		return fmt.Errorf("failed to cleanup old events: %w", err)
	}

	return nil
}
