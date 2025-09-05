package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/models"
)

var (
	ErrAccountNotFound = errors.New("account not found")
)

type AccountRepository interface {
	CreateAccount(ctx context.Context, userID string, currency models.Currency) (string, error)
	GetAccount(ctx context.Context, accountID string) (*models.Account, error)
	ListAccounts(ctx context.Context, userID string, limit, offset int) ([]*models.Account, error)
	UpdateAccount(ctx context.Context, account *models.Account) error
	DeleteAccount(ctx context.Context, accountID string) error
}

type accountRepo struct {
	db *pgxpool.Pool
}

func NewAccountRepository(db *pgxpool.Pool) AccountRepository {
	return &accountRepo{db: db}
}

func (r *accountRepo) CreateAccount(ctx context.Context, userID string, currency models.Currency) (string, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var accountID string
	err = tx.QueryRow(ctx, `
		INSERT INTO accounts (user_id, currency) 
		VALUES ($1, $2) 
		RETURNING account_id
	`, userID, currency.String()).Scan(&accountID)

	if err != nil {
		return "", err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balances (account_id, amount) 
		VALUES ($1, 0)
	`, accountID)

	if err != nil {
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	return accountID, nil
}

func (r *accountRepo) GetAccount(ctx context.Context, accountID string) (*models.Account, error) {
	var account models.Account
	var currencyStr string

	err := r.db.QueryRow(ctx, `
		SELECT account_id, user_id, currency, created_at, updated_at 
		FROM accounts 
		WHERE account_id = $1
	`, accountID).Scan(
		&account.AccountID,
		&account.UserID,
		&currencyStr,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, err
	}

	currency, err := models.CurrencyFromString(currencyStr)
	if err != nil {
		return nil, err
	}
	account.Currency = currency

	return &account, nil
}

func (r *accountRepo) ListAccounts(ctx context.Context, userID string, limit, offset int) ([]*models.Account, error) {
	rows, err := r.db.Query(ctx, `
		SELECT account_id, user_id, currency, created_at, updated_at 
		FROM accounts 
		WHERE user_id = $1 
		ORDER BY created_at DESC 
		LIMIT $2 OFFSET $3
	`, userID, limit, offset)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*models.Account
	for rows.Next() {
		var account models.Account
		var currencyStr string

		if err := rows.Scan(
			&account.AccountID,
			&account.UserID,
			&currencyStr,
			&account.CreatedAt,
			&account.UpdatedAt,
		); err != nil {
			return nil, err
		}

		currency, err := models.CurrencyFromString(currencyStr)
		if err != nil {
			return nil, err
		}
		account.Currency = currency

		accounts = append(accounts, &account)
	}

	return accounts, rows.Err()
}

func (r *accountRepo) UpdateAccount(ctx context.Context, account *models.Account) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
		UPDATE accounts 
		SET currency = $1, updated_at = now() 
		WHERE account_id = $2
	`, account.Currency.String(), account.AccountID)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrAccountNotFound
	}

	return tx.Commit(ctx)
}

func (r *accountRepo) DeleteAccount(ctx context.Context, accountID string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `DELETE FROM balances WHERE account_id = $1`, accountID)
	if err != nil {
		return err
	}

	result, err := tx.Exec(ctx, `DELETE FROM accounts WHERE account_id = $1`, accountID)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrAccountNotFound
	}

	return tx.Commit(ctx)
}
