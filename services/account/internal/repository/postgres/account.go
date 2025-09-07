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

type accountRepo struct {
	pool *pgxpool.Pool
}

func NewAccountRepository(pool *pgxpool.Pool) repository.AccountRepository {
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
		insert into accounts (user_id, currency) 
		values ($1, $2) 
		returning account_id
	`, userID, currency.String()).Scan(&accountID)

	if err != nil {
		return "", err
	}

	_, err = tx.Exec(ctx, `
		insert into balances (account_id, amount) 
		values ($1, 0)
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
		select account_id, user_id, currency, created_at, updated_at 
		from accounts 
		where account_id = $1
	`, accountID).Scan(
		&account.AccountID,
		&account.UserID,
		&currencyStr,
		&account.CreatedAt,
		&account.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, repository.ErrAccountNotFound
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

	var lastCreatedAt, lastAccountID, userIDparam interface{}
	if pageToken != "" {
		var decodeErr error
		lastCreatedAt, lastAccountID, decodeErr = repository.DecodePageToken(pageToken)
		if decodeErr != nil {
			return nil, "", fmt.Errorf("invalid page token: %w", decodeErr)
		}
	}

	if userID != "" {
		userIDparam = userID
	}

	rows, err := r.pool.Query(ctx, `
		select account_id, user_id, currency, created_at, updated_at 
		from accounts 
		where ((created_at, account_id) > ($1, $2) or ($1 is null and $2 is null))
		  and (user_id = $3 or $3 is null) 
		order by created_at asc, account_id asc
		limit $4
	`, lastCreatedAt, lastAccountID, userIDparam, pageSize+1)

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
		nextPageToken, err = repository.EncodePageToken(lastAccount.CreatedAt, lastAccount.AccountID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to encode page token: %w", err)
		}
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
		update accounts 
		set currency = $1, updated_at = now() 
		where account_id = $2
	`, account.Currency.String(), account.AccountID)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return repository.ErrAccountNotFound
	}

	return tx.Commit(ctx)
}

func (r *accountRepo) DeleteAccount(ctx context.Context, accountID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `delete from balances where account_id = $1`, accountID)
	if err != nil {
		return err
	}

	result, err := tx.Exec(ctx, `delete from accounts where account_id = $1`, accountID)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return repository.ErrAccountNotFound
	}

	return tx.Commit(ctx)
}
