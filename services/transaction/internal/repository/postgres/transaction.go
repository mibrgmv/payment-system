package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/shared/pagination"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
)

type transactionRepo struct {
	pool *pgxpool.Pool
}

func NewTransactionRepository(pool *pgxpool.Pool) repository.TransactionRepository {
	return &transactionRepo{pool: pool}
}

func (r *transactionRepo) CreateTransaction(ctx context.Context, transaction *models.Transaction) error {
	sql := `
	insert into transactions (
		transaction_id, type, from_account_id, to_account_id, 
		amount, currency, status, idempotency_key, error_message
	) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := r.pool.Exec(ctx, sql,
		transaction.TransactionID,
		transaction.Type.String(),
		transaction.FromAccountID,
		transaction.ToAccountID,
		transaction.Amount,
		transaction.Currency.String(),
		transaction.Status.String(),
		transaction.IdempotencyKey,
		transaction.ErrorMessage,
	)

	return err
}

func (r *transactionRepo) GetTransaction(ctx context.Context, transactionID string) (*models.Transaction, error) {
	sql := `
	select 
		transaction_id, type, from_account_id, to_account_id, 
		amount, currency, status, idempotency_key, error_message,
		created_at, updated_at, completed_at
	from transactions 
	where transaction_id = $1
	`

	var transaction models.Transaction
	var typeStr, currencyStr, statusStr string
	var fromAccountID, toAccountID *string
	var errorMessage *string
	var completedAt *time.Time

	err := r.pool.QueryRow(ctx, sql, transactionID).Scan(
		&transaction.TransactionID,
		&typeStr,
		&fromAccountID,
		&toAccountID,
		&transaction.Amount,
		&currencyStr,
		&statusStr,
		&transaction.IdempotencyKey,
		&errorMessage,
		&transaction.CreatedAt,
		&transaction.UpdatedAt,
		&completedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, repository.ErrTransactionNotFound
	}
	if err != nil {
		return nil, err
	}

	transactionType, err := models.TransactionTypeFromString(typeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction type in database: %w", err)
	}
	transaction.Type = transactionType

	currency, err := models.CurrencyFromString(currencyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid currency in database: %w", err)
	}
	transaction.Currency = currency

	status, err := models.TransactionStatusFromString(statusStr)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction status in database: %w", err)
	}
	transaction.Status = status

	transaction.FromAccountID = fromAccountID
	transaction.ToAccountID = toAccountID
	transaction.ErrorMessage = errorMessage
	transaction.CompletedAt = completedAt

	return &transaction, nil
}

func (r *transactionRepo) ListTransactions(ctx context.Context, filters models.TransactionFilters, pageSize int32, pageToken string) ([]*models.Transaction, string, error) {
	if pageSize == 0 {
		return []*models.Transaction{}, "", nil
	}

	sql := `
	select 
		transaction_id, type, from_account_id, to_account_id, 
		amount, currency, status, idempotency_key, error_message,
		created_at, updated_at, completed_at
	from transactions 
	where (created_at, transaction_id) > ($1, $2)
	  and ($3::uuid[] is null or from_account_id = any($3::uuid[]) or to_account_id = any($3::uuid[]))
	  and ($4::transaction_type[] is null or type = any($4::transaction_type[]))
	  and ($5::transaction_status[] is null or status = any($5::transaction_status[]))
	  and ($6::timestamptz is null or created_at >= $6)
	  and ($7::timestamptz is null or created_at <= $7)
	order by created_at asc, transaction_id asc
	limit $8
	`

	var lastCreatedAt, lastTransactionID interface{} = nil, nil
	if pageToken != "" {
		var err error
		lastCreatedAt, lastTransactionID, err = pagination.DecodePageToken(pageToken)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page token: %w", err)
		}
	}

	var accountIDs, types, statuses interface{}
	var fromDate, toDate interface{}

	if len(filters.AccountIDs) > 0 {
		accountIDs = filters.AccountIDs
	}
	if filters.Type != nil {
		types = []string{filters.Type.String()}
	}
	if filters.Status != nil {
		statuses = []string{filters.Status.String()}
	}
	if !filters.FromDate.IsZero() {
		fromDate = filters.FromDate
	}
	if !filters.ToDate.IsZero() {
		toDate = filters.ToDate
	}

	rows, err := r.pool.Query(ctx, sql,
		lastCreatedAt, lastTransactionID,
		accountIDs, types, statuses, fromDate, toDate,
		pageSize+1,
	)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var transaction models.Transaction
		var typeStr, currencyStr, statusStr string
		var fromAccountID, toAccountID *string
		var errorMessage *string
		var completedAt *time.Time

		if err := rows.Scan(
			&transaction.TransactionID,
			&typeStr,
			&fromAccountID,
			&toAccountID,
			&transaction.Amount,
			&currencyStr,
			&statusStr,
			&transaction.IdempotencyKey,
			&errorMessage,
			&transaction.CreatedAt,
			&transaction.UpdatedAt,
			&completedAt,
		); err != nil {
			return nil, "", err
		}

		transactionType, err := models.TransactionTypeFromString(typeStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid transaction type in database: %w", err)
		}
		transaction.Type = transactionType

		currency, err := models.CurrencyFromString(currencyStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid currency in database: %w", err)
		}
		transaction.Currency = currency

		status, err := models.TransactionStatusFromString(statusStr)
		if err != nil {
			return nil, "", fmt.Errorf("invalid transaction status in database: %w", err)
		}
		transaction.Status = status

		transaction.FromAccountID = fromAccountID
		transaction.ToAccountID = toAccountID
		transaction.ErrorMessage = errorMessage
		transaction.CompletedAt = completedAt

		transactions = append(transactions, &transaction)
	}

	var nextPageToken string
	if len(transactions) > int(pageSize) {
		transactions = transactions[:pageSize]
		lastTransaction := transactions[len(transactions)-1]
		nextPageToken, err = pagination.EncodePageToken(lastTransaction.CreatedAt, lastTransaction.TransactionID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return transactions, nextPageToken, nil
}

func (r *transactionRepo) UpdateTransactionStatus(ctx context.Context, transactionID string, status models.TransactionStatus, errorMessage *string) error {
	sql := `
	update transactions 
	set status = $1, error_message = $2, updated_at = now()
	where transaction_id = $3
	`

	result, err := r.pool.Exec(ctx, sql, status.String(), errorMessage, transactionID)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return repository.ErrTransactionNotFound
	}

	return nil
}

func (r *transactionRepo) CancelTransaction(ctx context.Context, transactionID string) error {
	sql := `
	update transactions 
	set status = 'cancelled', updated_at = now()
	where transaction_id = $1 and status in ('pending', 'processing')
	`

	result, err := r.pool.Exec(ctx, sql, transactionID)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return repository.ErrTransactionNotFound
	}

	return nil
}
