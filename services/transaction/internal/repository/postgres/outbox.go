package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
)

type outboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) repository.OutboxRepository {
	return &outboxRepo{pool: pool}
}

func (r *outboxRepo) AddToOutboxTx(ctx context.Context, tx pgx.Tx, event events.OutboxEvent) error {
	query := `
		insert into outbox_events (
			event_id, 
			event_type, 
			payload, 
			created_at, 
			topic
		) values ($1, $2, $3, $4, $5)
	`

	_, err := tx.Exec(ctx, query,
		event.EventID,
		event.EventType,
		event.RawPayload,
		event.CreatedAt,
		event.Topic,
	)

	return err
}

func (r *outboxRepo) GetPendingEvents(ctx context.Context, limit int) ([]events.OutboxEvent, error) {
	query := `
		select event_id, event_type, payload, created_at, topic
		from outbox_events 
		where status = 'pending' 
		order by created_at 
		limit $1
	`

	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending events: %w", err)
	}
	defer rows.Close()

	var eventsArr []events.OutboxEvent
	for rows.Next() {
		var event events.OutboxEvent
		var payloadBytes []byte

		err := rows.Scan(&event.EventID, &event.EventType, &payloadBytes, &event.CreatedAt, &event.Topic)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}

		event.RawPayload = payloadBytes
		eventsArr = append(eventsArr, event)
	}

	return eventsArr, nil
}

func (r *outboxRepo) LockEventForProcessing(ctx context.Context, tx pgx.Tx, eventID string) (*events.OutboxEvent, error) {
	query := `
		select event_id, event_type, payload, created_at, topic
		from outbox_events 
		where event_id = $1 and status = 'pending'
		for update skip locked
	`

	var event events.OutboxEvent
	var payloadBytes []byte

	err := tx.QueryRow(ctx, query, eventID).Scan(
		&event.EventID,
		&event.EventType,
		&payloadBytes,
		&event.CreatedAt,
		&event.Topic,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan outbox event: %w", err)
	}

	event.RawPayload = payloadBytes
	return &event, nil
}

func (r *outboxRepo) MarkEventAsPublishedTx(ctx context.Context, tx pgx.Tx, eventID string) error {
	query := `
		update outbox_events 
		set status = 'published', published_at = now() 
		where event_id = $1
	`

	_, err := tx.Exec(ctx, query, eventID)
	return err
}

func (r *outboxRepo) MarkEventAsFailedTx(ctx context.Context, tx pgx.Tx, eventID string, errorMsg string) error {
	query := `
		update outbox_events 
		set status = 'failed', error_message = $2, retry_count = retry_count + 1
		where event_id = $1
	`

	_, err := tx.Exec(ctx, query, eventID, errorMsg)
	return err
}

func (r *outboxRepo) CleanupOldEvents(ctx context.Context, olderThanDays int) error {
	query := `
		delete from outbox_events 
		where status = 'published' 
		and published_at < now() - interval '1 day' * $1
	`

	_, err := r.pool.Exec(ctx, query, olderThanDays)
	return err
}
