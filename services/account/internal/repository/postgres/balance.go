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
	pool *pgxpool.Pool
}

func NewBalanceRepository(pool *pgxpool.Pool) repository.BalanceRepository {
	return &balanceRepo{pool: pool}
}

func (r *balanceRepo) GetBalance(ctx context.Context, accountID string) (*models.Balance, error) {
	var balance models.Balance
	var currencyStr string

	err := r.pool.QueryRow(ctx, `
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

func (r *balanceRepo) UpdateBalanceTx(ctx context.Context, tx pgx.Tx, accountID string, amount int64) (*models.Balance, error) {
	var balance models.Balance
	var currencyStr string

	err := tx.QueryRow(ctx, `
        update balances b
        set amount = amount + $2, last_updated = now()
        from accounts a
        where b.account_id = $1 
            and b.account_id = a.account_id
            and (b.amount + $2) >= 0
        returning b.account_id, b.amount, a.currency, b.last_updated
    `, accountID, amount).Scan(
		&balance.AccountID,
		&balance.Amount,
		&currencyStr,
		&balance.LastUpdated,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		var currentBalance int64
		checkErr := tx.QueryRow(ctx, `
            select amount from balances where account_id = $1
        `, accountID).Scan(&currentBalance)

		if errors.Is(checkErr, pgx.ErrNoRows) {
			return nil, repository.ErrBalanceNotFound
		}

		if currentBalance+amount < 0 {
			return nil, repository.ErrInsufficientBalance
		}

		return nil, repository.ErrBalanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to change balance: %w", err)
	}

	currency, err := models.CurrencyFromString(currencyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid currency in database: %w", err)
	}
	balance.Currency = currency

	return &balance, nil
}
