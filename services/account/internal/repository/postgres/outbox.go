package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type outboxRepo struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) repository.OutboxRepository {
	return &outboxRepo{pool: pool}
}

func (r *outboxRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

func (r *outboxRepo) AddToOutboxTx(ctx context.Context, tx pgx.Tx, event models.OutboxEvent) error {
	eventBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	query := `
		insert into outbox_events (
			event_id, 
			event_type, 
			payload, 
			created_at, 
			status,
			topic
		) values ($1, $2, $3, $4, $5, $6)
	`

	_, err = tx.Exec(ctx, query,
		event.EventID,
		event.EventType,
		eventBytes,
		event.CreatedAt,
		"pending",
		event.Topic,
	)

	return err
}

func (r *outboxRepo) GetPendingEventsForUpdateTx(ctx context.Context, tx pgx.Tx, limit int) ([]models.OutboxEvent, error) {
	query := `
		select event_id, event_type, payload, created_at, topic
		from outbox_events 
		where status = 'pending' 
		order by created_at 
		limit $1
		for update skip locked
	`

	rows, err := tx.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending events: %w", err)
	}
	defer rows.Close()

	var events []models.OutboxEvent
	for rows.Next() {
		var event models.OutboxEvent
		var payloadBytes []byte

		err := rows.Scan(&event.EventID, &event.EventType, &payloadBytes, &event.CreatedAt, &event.Topic)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}

		//switch event.EventType {
		//case "account_created":
		//	var payload models.AccountCreatedEvent
		//	if err := json.Unmarshal(payloadBytes, &payload); err == nil {
		//		event.Payload = payload
		//	}
		//case "balance_updated":
		//	var payload models.BalanceUpdatedEvent
		//	if err := json.Unmarshal(payloadBytes, &payload); err == nil {
		//		event.Payload = payload
		//	}
		//case "insufficient_funds":
		//	var payload models.InsufficientFundsEvent
		//	if err := json.Unmarshal(payloadBytes, &payload); err == nil {
		//		event.Payload = payload
		//	}
		//}

		event.Payload = payloadBytes
		events = append(events, event)
	}

	return events, nil
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
