package repository

import (
	"context"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, accountID string) (*models.Balance, error)
}
