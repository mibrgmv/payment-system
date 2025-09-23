package consumer_handlers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	"github.com/mibrgmv/payment-service/shared/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestContainer(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgrestest.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgrestest.WithDatabase("testdb"),
		postgrestest.WithUsername("testuser"),
		postgrestest.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	err = runMigrations(pool)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

func runMigrations(pool *pgxpool.Pool) error {
	ctx := context.Background()

	queries := []string{
		`create type transaction_type as enum (
    		'transfer',
    		'deposit', 
    		'withdrawal'
		)`,
		`create type transaction_status as enum (
    		'pending',
    		'processing',
    		'completed', 
    		'failed',
    		'cancelled'
		)`,
		`create type currency_code as enum (
    		'RUB',
    		'USD',
    		'EUR'
		)`,
		`create table if not exists transactions (
			transaction_id uuid primary key default gen_random_uuid(),
			type transaction_type not null,
			from_account_id uuid,
			to_account_id uuid,
			amount decimal(19, 4) not null check (amount > 0),
			currency currency_code not null,
			status transaction_status not null default 'pending',
			idempotency_key varchar(255) not null unique,
			error_message text,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now(),
			completed_at timestamptz,
			
			constraint valid_from_account check (
				(type in ('transfer', 'withdrawal') and from_account_id is not null) or
				(type = 'deposit' and from_account_id is null)
			),
			constraint valid_to_account check (
				(type in ('transfer', 'deposit') and to_account_id is not null) or
				(type = 'withdrawal' and to_account_id is null)
			),
			constraint valid_account_combination check (
				from_account_id is distinct from to_account_id or
				(from_account_id is null and to_account_id is null)
			)
		)`,
		`create table if not exists processed_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			source_service varchar(100) not null,
			processed_at timestamptz not null default now()
		)`,
		`create table if not exists outbox_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			topic varchar(255) not null,
			payload jsonb not null,
			status varchar(50) not null default 'pending',
			retry_count integer not null default 0,
			max_retries integer not null default 5,
			error_message text,
			created_at timestamptz not null default now(),
			published_at timestamptz,
			updated_at timestamptz not null default now(),
			next_retry_at timestamptz
		)`,
	}

	for _, query := range queries {
		_, err := pool.Exec(ctx, query)
		if err != nil {
			return err
		}
	}

	return nil
}

func setupTestTransaction(t *testing.T, pool *pgxpool.Pool, transactionID string, status string) {
	ctx := context.Background()

	fromAccountID := uuid.New()
	toAccountID := uuid.New()

	_, err := pool.Exec(ctx,
		`INSERT INTO transactions (transaction_id, type, from_account_id, to_account_id, amount, currency, status, idempotency_key) 
		 VALUES ($1, 'transfer', $2, $3, 100.00, 'USD', $4, $5)`,
		transactionID, fromAccountID, toAccountID, status, transactionID)
	require.NoError(t, err)
}

func TestTransactionResultHandler_HandleEvent_Completed_Success(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	transactionID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, "pending")

	event := events.TransactionResult{
		EventID:       "test_event_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "completed",
		FailureReason: "",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	assert.Equal(t, "transaction_result", handler.GetEventType())

	eventID, err := handler.GetEventID(eventBytes)
	require.NoError(t, err)
	assert.Equal(t, event.EventID, eventID)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, eventBytes)
	assert.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	transaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "completed", transaction.Status.String())
	assert.NotNil(t, transaction.CompletedAt)
}

func TestTransactionResultHandler_HandleEvent_Failed_Success(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	transactionID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, "pending")

	failureReason := "insufficient funds"
	event := events.TransactionResult{
		EventID:       "failed_event_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "failed",
		FailureReason: failureReason,
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, eventBytes)
	assert.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	transaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "failed", transaction.Status.String())
	assert.Equal(t, failureReason, *transaction.ErrorMessage)
	assert.NotNil(t, transaction.CompletedAt)
}

func TestTransactionResultHandler_HandleEvent_TransactionNotFound(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	nonExistentTransactionID := uuid.New().String()
	event := events.TransactionResult{
		EventID:       "not_found_event_" + uuid.New().String(),
		TransactionID: nonExistentTransactionID,
		Status:        "completed",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, eventBytes)
	assert.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

func TestTransactionResultHandler_HandleEvent_InvalidStatus(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	transactionID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, "pending")

	event := events.TransactionResult{
		EventID:       "invalid_status_event_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "invalid_status",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, eventBytes)
	assert.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	transaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "failed", transaction.Status.String())
	assert.Contains(t, *transaction.ErrorMessage, service.ErrInvalidTransactionStatus.Error())
}

func TestTransactionResultHandler_HandleEvent_AlreadyCompleted(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	transactionID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, "completed")

	event := events.TransactionResult{
		EventID:       "already_completed_event_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "completed",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, eventBytes)
	assert.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	transaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "completed", transaction.Status.String())
}

func TestTransactionResultHandler_HandleEvent_InvalidJSON(t *testing.T) {
	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	ctx := context.Background()

	invalidJSON := []byte(`{"invalid": "json" missing closing brace`)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, invalidJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transaction result")
}

func TestTransactionResultHandler_GetEventID_Success(t *testing.T) {
	transactionRepo := postgres.NewTransactionRepository(nil)
	outboxRepo := outbox.NewPostgresRepository(nil)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	eventData := []byte(`{"event_id": "test-event-123", "transaction_id": "txn-456"}`)

	eventID, err := handler.GetEventID(eventData)
	require.NoError(t, err)
	assert.Equal(t, "test-event-123", eventID)
}

func TestTransactionResultHandler_GetEventID_InvalidJSON(t *testing.T) {
	transactionRepo := postgres.NewTransactionRepository(nil)
	outboxRepo := outbox.NewPostgresRepository(nil)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionResultHandler(transactionService)

	invalidJSON := []byte(`{"invalid": "json"`)

	_, err := handler.GetEventID(invalidJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to extract event ID")
}
