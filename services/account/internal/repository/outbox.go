package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
)

type OutboxRepository interface {
	AddToOutboxTx(ctx context.Context, tx pgx.Tx, event events.OutboxEvent) error
	GetPendingEvents(ctx context.Context, limit int) ([]events.OutboxEvent, error)
	LockEventForProcessing(ctx context.Context, tx pgx.Tx, eventID string) (*events.OutboxEvent, error)
	MarkEventAsPublishedTx(ctx context.Context, tx pgx.Tx, eventID string) error
	MarkEventAsFailedTx(ctx context.Context, tx pgx.Tx, eventID string, errorMsg string) error
	CleanupOldEvents(ctx context.Context, olderThanDays int) error
}
