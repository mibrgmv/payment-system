package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-system/account/internal/service/models"
)

var (
	ErrBalanceNotFound     = errors.New("balance not found")
	ErrInsufficientBalance = errors.New("insufficient balance")
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, accountID string) (*models.Balance, error)
	GetBalanceTx(ctx context.Context, tx pgx.Tx, accountID string) (*models.Balance, error)
	UpdateBalanceTx(ctx context.Context, tx pgx.Tx, accountID string, amount float64) (*models.Balance, error)
}
