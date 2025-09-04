package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type BalanceRepository interface {
	GetBalance(ctx context.Context, accountID string) (*Balance, error)
}

type Balance struct {
	AccountID   string    `db:"account_id"`
	Amount      float64   `db:"amount"`
	Currency    string    `db:"currency"`
	LastUpdated time.Time `db:"last_updated"`
}

type balanceRepo struct {
	db *pgxpool.Pool
}

func NewBalanceRepository(db *pgxpool.Pool) BalanceRepository {
	return &balanceRepo{db: db}
}

func (r *balanceRepo) GetBalance(ctx context.Context, accountID string) (*Balance, error) {
	var balance Balance
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
