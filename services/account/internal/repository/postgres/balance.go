package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type balanceRepo struct {
	db *pgxpool.Pool
}

func NewBalanceRepository(db *pgxpool.Pool) repository.BalanceRepository {
	return &balanceRepo{db: db}
}

func (r *balanceRepo) GetBalance(ctx context.Context, accountID string) (*models.Balance, error) {
	var balance models.Balance
	err := r.db.QueryRow(ctx, `
		select b.account_id, b.amount, a.currency, b.last_updated 
		from balances b
		join accounts a on b.account_id = a.account_id 
		where b.account_id = $1
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
