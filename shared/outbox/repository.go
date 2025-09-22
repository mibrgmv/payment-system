package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Repository interface {
	AddToOutboxTx(ctx context.Context, tx pgx.Tx, event Event) error
	GetPendingEvents(ctx context.Context, limit int) ([]Event, error)
	GetRetryEvents(ctx context.Context, limit int) ([]Event, error)
	LockEventForProcessing(ctx context.Context, tx pgx.Tx, eventID string) (*Event, error)
	MarkEventAsPublishedTx(ctx context.Context, tx pgx.Tx, eventID string) error
	MarkEventAsFailedTx(ctx context.Context, tx pgx.Tx, eventID string, errorMsg string, nextRetryAt time.Time) error
	CleanupOldEvents(ctx context.Context, olderThanDays int) error
}
