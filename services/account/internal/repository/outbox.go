package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type OutboxRepository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	AddToOutboxTx(ctx context.Context, tx pgx.Tx, event models.OutboxEvent) error
	GetPendingEventsForUpdateTx(ctx context.Context, tx pgx.Tx, limit int) ([]models.OutboxEvent, error)
	MarkEventAsPublishedTx(ctx context.Context, tx pgx.Tx, eventID string) error
	MarkEventAsFailedTx(ctx context.Context, tx pgx.Tx, eventID string, errorMsg string) error
	CleanupOldEvents(ctx context.Context, olderThanDays int) error
}
