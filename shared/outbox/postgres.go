package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) Repository {
	return &postgresRepo{pool: pool}
}

func (r *postgresRepo) AddToOutboxTx(ctx context.Context, tx pgx.Tx, event Event) error {
	query := `
		insert into outbox_events (
			event_id, 
			event_type, 
			payload, 
			created_at, 
			topic,
		    max_retries
		) values ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.Exec(ctx, query,
		event.EventID,
		event.EventType,
		event.RawPayload,
		event.CreatedAt,
		event.Topic,
		event.MaxRetries,
	)

	return err
}

func (r *postgresRepo) GetPendingEvents(ctx context.Context, limit int) ([]Event, error) {
	query := `
		select event_id, event_type, payload, created_at, topic,
		       status, retry_count, max_retries, error_message, next_retry_at
		from outbox_events 
		where status = 'pending'
		order by created_at 
		limit $1
	`

	return r.queryEvents(ctx, query, limit)
}

func (r *postgresRepo) GetRetryEvents(ctx context.Context, limit int) ([]Event, error) {
	query := `
		select event_id, event_type, payload, created_at, topic,
		       status, retry_count, max_retries, error_message, next_retry_at
		from outbox_events 
		where status = 'failed' 
		and next_retry_at <= NOW()
		and retry_count < max_retries
		order by next_retry_at
		limit $1
	`

	return r.queryEvents(ctx, query, limit)
}

func (r *postgresRepo) queryEvents(ctx context.Context, query string, limit int) ([]Event, error) {
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := r.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}

	return events, nil
}

func (r *postgresRepo) LockEventForProcessing(ctx context.Context, tx pgx.Tx, eventID string) (*Event, error) {
	query := `
		select event_id, event_type, payload, created_at, topic,
		       status, retry_count, max_retries, error_message, next_retry_at
		from outbox_events 
		where event_id = $1 
		  and status in ('pending', 'failed') 
		  and (next_retry_at is null or next_retry_at <= now())
		  and retry_count < max_retries
		for update skip locked
	`

	var event Event
	var payloadBytes []byte
	var errorMessage *string
	var nextRetryAt *time.Time
	err := tx.QueryRow(ctx, query, eventID).Scan(
		&event.EventID,
		&event.EventType,
		&payloadBytes,
		&event.CreatedAt,
		&event.Topic,
		&event.Status,
		&event.RetryCount,
		&event.MaxRetries,
		&errorMessage,
		&nextRetryAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan outbox event: %w", err)
	}

	event.RawPayload = payloadBytes
	event.ErrorMessage = errorMessage
	event.NextRetryAt = nextRetryAt
	return &event, nil
}

func (r *postgresRepo) MarkEventAsPublishedTx(ctx context.Context, tx pgx.Tx, eventID string) error {
	query := `
		update outbox_events 
		set status = 'published',
		    updated_at = now(),
		    published_at = now()
		where event_id = $1
	`

	_, err := tx.Exec(ctx, query, eventID)
	return err
}

func (r *postgresRepo) MarkEventAsFailedTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
	errorMsg string,
	nextRetryAt time.Time,
) error {
	query := `
		update outbox_events 
		set status = 'failed',
		    error_message = $2,
		    retry_count = retry_count + 1,
		    next_retry_at = $3,
		    updated_at = now()
		where event_id = $1
	`

	_, err := tx.Exec(ctx, query, eventID, errorMsg, nextRetryAt)
	return err
}

func (r *postgresRepo) CleanupOldEvents(ctx context.Context, olderThanDays int) error {
	query := `
		delete from outbox_events 
		where (status = 'published' and 
		       published_at < now() - interval '1 day' * $1)
		   or (status = 'failed' and
		       retry_count >= max_retries
		       created_at < now() - interval '1 day' * $1)
	`

	_, err := r.pool.Exec(ctx, query, olderThanDays)
	return err
}

func (r *postgresRepo) scanEvent(rows pgx.Rows) (*Event, error) {
	var event Event
	var payloadBytes []byte
	var errorMessage *string
	var nextRetryAt *time.Time

	err := rows.Scan(
		&event.EventID,
		&event.EventType,
		&payloadBytes,
		&event.CreatedAt,
		&event.Topic,
		&event.Status,
		&event.RetryCount,
		&event.MaxRetries,
		&errorMessage,
		&nextRetryAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan outbox event: %w", err)
	}

	event.RawPayload = payloadBytes
	event.ErrorMessage = errorMessage
	event.NextRetryAt = nextRetryAt
	return &event, nil
}
