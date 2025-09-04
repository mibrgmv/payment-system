package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrAccountNotFound = errors.New("account not found")
)

type AccountRepository interface {
	CreateAccount(ctx context.Context, userID string, currency string) (string, error)
	GetAccount(ctx context.Context, accountID string) (*Account, error)
	ListAccounts(ctx context.Context, userID string, limit, offset int) ([]*Account, error)
	UpdateAccount(ctx context.Context, account *Account) error
	DeleteAccount(ctx context.Context, accountID string) error
}

type Account struct {
	AccountID string    `db:"account_id"`
	UserID    string    `db:"user_id"`
	Currency  string    `db:"currency"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type accountRepo struct {
	db *pgxpool.Pool
}

func NewAccountRepository(db *pgxpool.Pool) AccountRepository {
	return &accountRepo{db: db}
}

func (r *accountRepo) CreateAccount(ctx context.Context, userID string, currency string) (string, error) {
	var accountID string
	err := r.db.QueryRow(ctx, `
		INSERT INTO accounts (user_id, currency) 
		VALUES ($1, $2) 
		RETURNING account_id
	`, userID, currency).Scan(&accountID)

	if err != nil {
		return "", err
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO balances (account_id, amount) 
		VALUES ($1, 0)
	`, accountID)

	return accountID, err
}

func (r *accountRepo) GetAccount(ctx context.Context, accountID string) (*Account, error) {
	var account Account
	err := r.db.QueryRow(ctx, `
		SELECT account_id, user_id, currency, created_at, updated_at 
		FROM accounts 
		WHERE account_id = $1
	`, accountID).Scan(
		&account.AccountID,
		&account.UserID,
		&account.Currency,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}

	return &account, err
}

func (r *accountRepo) ListAccounts(ctx context.Context, userID string, limit, offset int) ([]*Account, error) {
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

	var accounts []*Account
	for rows.Next() {
		var account Account
		if err := rows.Scan(
			&account.AccountID,
			&account.UserID,
			&account.Currency,
			&account.CreatedAt,
			&account.UpdatedAt,
		); err != nil {
			return nil, err
		}
		accounts = append(accounts, &account)
	}

	return accounts, rows.Err()
}

func (r *accountRepo) UpdateAccount(ctx context.Context, account *Account) error {
	result, err := r.db.Exec(ctx, `
		UPDATE accounts 
		SET currency = $1, updated_at = now() 
		WHERE account_id = $2
	`, account.Currency, account.AccountID)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrAccountNotFound
	}

	return nil
}

func (r *accountRepo) DeleteAccount(ctx context.Context, accountID string) error {
	result, err := r.db.Exec(ctx, `
		DELETE FROM accounts 
		WHERE account_id = $1
	`, accountID)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrAccountNotFound
	}

	return nil
}
