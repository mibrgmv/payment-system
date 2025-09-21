package inbox

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	ErrEventAlreadyProcessed = errors.New("event already processed")
)

type Repository interface {
	MarkEventProcessedTx(ctx context.Context, tx pgx.Tx, eventID, eventType string) error
	IsEventProcessed(ctx context.Context, eventID string) (bool, error)
	CleanupOldEvents(ctx context.Context, olderThanDays int) error
}
