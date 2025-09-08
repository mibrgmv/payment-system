package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
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
	var currencyStr string

	err := r.db.QueryRow(ctx, `
		select b.account_id, b.amount, a.currency, b.last_updated 
		from balances b
		join accounts a on b.account_id = a.account_id 
		where b.account_id = $1
	`, accountID).Scan(
		&balance.AccountID,
		&balance.Amount,
		&currencyStr,
		&balance.LastUpdated,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, repository.ErrBalanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}

	currency, err := models.CurrencyFromString(currencyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid currency in database: %w", err)
	}
	balance.Currency = currency

	return &balance, nil
}
