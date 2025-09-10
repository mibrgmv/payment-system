package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
	"github.com/mibrgmv/payment-service/shared/pagination"
)

type transactionRepo struct {
	pool *pgxpool.Pool
}

func NewTransactionRepository(pool *pgxpool.Pool) repository.TransactionRepository {
	return &transactionRepo{pool: pool}
}

func (r *transactionRepo) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

func (r *transactionRepo) CreateTransactionTx(ctx context.Context, tx pgx.Tx, transaction *models.Transaction) error {
	sql := `
        insert into transactions (
            transaction_id, type, from_account_id, to_account_id, 
            amount, currency, status, idempotency_key, error_message,
            created_at, updated_at
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `

	_, err := tx.Exec(ctx, sql,
		transaction.TransactionID,
		transaction.Type.String(),
		transaction.FromAccountID,
		transaction.ToAccountID,
		transaction.Amount,
		transaction.Currency.String(),
		transaction.Status.String(),
		transaction.IdempotencyKey,
		transaction.ErrorMessage,
		transaction.CreatedAt,
		transaction.UpdatedAt,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: %v", repository.ErrIdempotencyConflict, err)
		}
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	return nil
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

	return r.scanTransaction(ctx, r.pool, sql, transactionID)
}

func (r *transactionRepo) GetTransactionByIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, idempotencyKey string) (*models.Transaction, error) {
	sql := `
        select transaction_id, type, from_account_id, to_account_id, 
               amount, currency, status, idempotency_key, error_message,
               created_at, updated_at, completed_at
        from transactions 
        where idempotency_key = $1
        for update
    `

	return r.scanTransaction(ctx, tx, sql, idempotencyKey)
}

func (r *transactionRepo) ListTransactions(
	ctx context.Context,
	filters models.TransactionFilters,
	pageSize int32,
	pageToken string,
) ([]*models.Transaction, string, error) {
	if pageSize == 0 {
		return []*models.Transaction{}, "", nil
	}

	sql := `
	select 
		transaction_id, type, from_account_id, to_account_id, 
		amount, currency, status, idempotency_key, error_message,
		created_at, updated_at, completed_at
	from transactions 
	where ((created_at, transaction_id) > ($1, $2) or ($1 is null and $2 is null))
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

		parsed, err := r.parseTransactionFields(&transaction, typeStr, currencyStr, statusStr, fromAccountID, toAccountID, errorMessage, completedAt)
		if err != nil {
			return nil, "", err
		}

		transactions = append(transactions, parsed)
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

func (r *transactionRepo) scanTransaction(ctx context.Context, querier interface{}, query string, args ...interface{}) (*models.Transaction, error) {
	var transaction models.Transaction
	var typeStr, currencyStr, statusStr string
	var fromAccountID, toAccountID *string
	var errorMessage *string
	var completedAt *time.Time

	var row pgx.Row
	switch q := querier.(type) {
	case *pgxpool.Pool:
		row = q.QueryRow(ctx, query, args...)
	case pgx.Tx:
		row = q.QueryRow(ctx, query, args...)
	default:
		return nil, fmt.Errorf("unsupported querier type")
	}

	err := row.Scan(
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
		return nil, fmt.Errorf("database query failed: %w", err)
	}

	return r.parseTransactionFields(&transaction, typeStr, currencyStr, statusStr, fromAccountID, toAccountID, errorMessage, completedAt)
}

func (r *transactionRepo) parseTransactionFields(
	transaction *models.Transaction,
	typeStr, currencyStr, statusStr string,
	fromAccountID, toAccountID *string,
	errorMessage *string,
	completedAt *time.Time,
) (*models.Transaction, error) {
	transactionType, err := models.TransactionTypeFromString(typeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction type '%s' in database: %w", typeStr, err)
	}
	transaction.Type = transactionType

	currency, err := models.CurrencyFromString(currencyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid currency '%s' in database: %w", currencyStr, err)
	}
	transaction.Currency = currency

	status, err := models.TransactionStatusFromString(statusStr)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction status '%s' in database: %w", statusStr, err)
	}
	transaction.Status = status

	transaction.FromAccountID = fromAccountID
	transaction.ToAccountID = toAccountID
	transaction.ErrorMessage = errorMessage
	transaction.CompletedAt = completedAt

	return transaction, nil
}
