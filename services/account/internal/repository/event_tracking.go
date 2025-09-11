package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	ErrEventAlreadyProcessed = errors.New("event already processed")
)

type EventTrackingRepository interface {
	MarkEventProcessed(ctx context.Context, tx pgx.Tx, eventID string, accountID string) error
	IsEventProcessed(ctx context.Context, eventID string) (bool, error)
}
