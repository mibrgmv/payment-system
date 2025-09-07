package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, accountID string) (*models.Balance, error)
}

type balanceRepo struct {
	db *pgxpool.Pool
}

func NewBalanceRepository(db *pgxpool.Pool) BalanceRepository {
	return &balanceRepo{db: db}
}

func (r *balanceRepo) GetBalance(ctx context.Context, accountID string) (*models.Balance, error) {
	var balance models.Balance
	err := r.db.QueryRow(ctx, `
		SELECT b.account_id, b.amount, a.currency, b.last_updated 
		FROM balances b
		JOIN accounts a ON b.account_id = a.account_id 
		WHERE b.account_id = $1
	`, accountID).Scan(
		&balance.AccountID,
		&balance.Amount,
		&balance.Currency,
		&balance.LastUpdated,
	)

	if err != nil {
		return nil, err
	}

	return &balance, nil
}
