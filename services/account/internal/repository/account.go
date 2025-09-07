package repository

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
	"strings"
	"time"
)

var (
	ErrAccountNotFound = errors.New("account not found")
)

type AccountRepository interface {
	CreateAccount(ctx context.Context, userID string, currency models.Currency) (string, error)
	GetAccount(ctx context.Context, accountID string) (*models.Account, error)
	ListAccounts(ctx context.Context, userID string, pageSize int32, pageToken string) ([]*models.Account, string, error)
	UpdateAccount(ctx context.Context, account *models.Account) error
	DeleteAccount(ctx context.Context, accountID string) error
}

type accountRepo struct {
	pool *pgxpool.Pool
}

func NewAccountRepository(pool *pgxpool.Pool) AccountRepository {
	return &accountRepo{pool: pool}
}

func (r *accountRepo) CreateAccount(ctx context.Context, userID string, currency models.Currency) (string, error) {
	tx, err := r.pool.Begin(ctx)
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

	err := r.pool.QueryRow(ctx, `
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

func (r *accountRepo) ListAccounts(
	ctx context.Context,
	userID string,
	pageSize int32,
	pageToken string,
) ([]*models.Account, string, error) {
	if pageSize == 0 {
		return []*models.Account{}, "", nil
	}

	sql := `
	select account_id, user_id, currency, created_at, updated_at 
	from accounts 
	where (created_at, account_id) > ($1, $2)
	  and ($3 = '' or user_id = $3::uuid) 
	order by created_at asc, account_id asc
	limit $4
	`

	lastCreatedAt := time.Time{}
	lastAccountID := uuid.Nil

	if pageToken != "" {
		var decodeErr error
		lastCreatedAt, lastAccountID, decodeErr = decodePageToken(pageToken)
		if decodeErr != nil {
			return nil, "", fmt.Errorf("invalid page token: %w", decodeErr)
		}
	}

	rows, err := r.pool.Query(ctx, sql, lastCreatedAt, lastAccountID, userID, pageSize+1)
	if err != nil {
		return nil, "", err
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
			return nil, "", err
		}

		currency, err := models.CurrencyFromString(currencyStr)
		if err != nil {
			return nil, "", err
		}
		account.Currency = currency

		accounts = append(accounts, &account)
	}

	var nextPageToken string
	if len(accounts) > int(pageSize) {
		accounts = accounts[:pageSize]
		lastAccount := accounts[len(accounts)-1]
		nextPageToken = encodePageToken(lastAccount.CreatedAt, lastAccount.AccountID)
	}

	return accounts, nextPageToken, nil
}

func (r *accountRepo) UpdateAccount(ctx context.Context, account *models.Account) error {
	tx, err := r.pool.Begin(ctx)
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
	tx, err := r.pool.Begin(ctx)
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

func encodePageToken(createdAt time.Time, accountID string) string {
	data := fmt.Sprintf("%s|%s", createdAt.Format(time.RFC3339Nano), accountID)
	return base64.StdEncoding.EncodeToString([]byte(data))
}

func decodePageToken(token string) (time.Time, uuid.UUID, error) {
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}

	parts := strings.Split(string(decoded), "|")
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid token format")
	}

	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}

	accountId, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}

	return createdAt, accountId, nil
}
