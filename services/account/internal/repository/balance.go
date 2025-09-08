package repository

import (
	"context"
	"errors"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

var (
	ErrBalanceNotFound = errors.New("balance not found")
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, accountID string) (*models.Balance, error)
}
