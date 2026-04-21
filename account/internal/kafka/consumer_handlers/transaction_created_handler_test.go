package consumer_handlers_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/outbox"
	platformpostgres "github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/payment-system/account/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-system/account/internal/kafka/events"
	"github.com/mibrgmv/payment-system/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/account/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestContainer(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgrestest.Run(ctx,
		"postgres:16-alpine",
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

	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)
	migrationPath := filepath.Join(basepath, "..", "..", "..", "migrations")
	if err := platformpostgres.MigrateUp(connStr, migrationPath); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

func setupTestAccount(t *testing.T, pool *pgxpool.Pool, accountID, userID string, balance float64) {
	ctx := context.Background()

	_, err := pool.Exec(ctx,
		`insert into accounts (account_id, user_id, currency) values ($1, $2, 'USD')`,
		accountID, userID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`insert into balances (account_id, amount) values ($1, $2)`,
		accountID, balance)
	require.NoError(t, err)
}

func TestTransactionCreatedHandler_HandleEvent_Transfer_Success(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	ctx := context.Background()

	fromAccountID := uuid.New().String()
	toAccountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := 10000.00
	transferAmount := 2500.00

	setupTestAccount(t, pool, fromAccountID, userID, initialBalance)
	setupTestAccount(t, pool, toAccountID, userID, 5000.00)

	event := events.TransactionCreated{
		EventID:       "test_event_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "transfer",
		Amount:        transferAmount,
		Currency:      "USD",
		FromAccountID: fromAccountID,
		ToAccountID:   toAccountID,
		Description:   "Test transfer",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	assert.Equal(t, "transaction_created", handler.GetEventType())

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

	fromBalance, err := balanceRepo.GetBalance(ctx, fromAccountID)
	require.NoError(t, err)
	assert.Equal(t, 7500.00, fromBalance.Amount)

	toBalance, err := balanceRepo.GetBalance(ctx, toAccountID)
	require.NoError(t, err)
	assert.Equal(t, 7500.00, toBalance.Amount)

	outboxEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, outboxEvents, 1)
	assert.Equal(t, "transaction_result", outboxEvents[0].EventType)
}

func TestTransactionCreatedHandler_HandleEvent_Deposit_Success(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	ctx := context.Background()

	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := 5000.00
	depositAmount := 1000.00

	setupTestAccount(t, pool, accountID, userID, initialBalance)

	event := events.TransactionCreated{
		EventID:       "deposit_test_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "deposit",
		Amount:        depositAmount,
		Currency:      "USD",
		ToAccountID:   accountID,
		Description:   "Test deposit",
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

	balance, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	expectedBalance := initialBalance + depositAmount
	assert.Equal(t, expectedBalance, balance.Amount)
}

func TestTransactionCreatedHandler_HandleEvent_InsufficientFunds(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	ctx := context.Background()

	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := 500.00
	withdrawalAmount := 1000.00

	setupTestAccount(t, pool, accountID, userID, initialBalance)

	event := events.TransactionCreated{
		EventID:       "insufficient_funds_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "withdrawal",
		Amount:        withdrawalAmount,
		Currency:      "USD",
		FromAccountID: accountID,
		Description:   "Test withdrawal",
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

	balance, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, initialBalance, balance.Amount)

	outboxEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, outboxEvents, 1)
	assert.Equal(t, "transaction_result", outboxEvents[0].EventType)

	var resultEvent events.TransactionResult
	err = json.Unmarshal(outboxEvents[0].RawPayload, &resultEvent)
	require.NoError(t, err)
	assert.Equal(t, "failed", resultEvent.Status)
	assert.Contains(t, resultEvent.FailureReason, "insufficient funds")
}

func TestTransactionCreatedHandler_HandleEvent_AccountNotFound(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	ctx := context.Background()

	nonExistentAccountID := uuid.New().String()
	event := events.TransactionCreated{
		EventID:       "nonexistent_account_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "deposit",
		Amount:        1000.00,
		Currency:      "USD",
		ToAccountID:   nonExistentAccountID,
		Description:   "Test deposit to nonexistent account",
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

	outboxEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, outboxEvents, 1)
	assert.Equal(t, "transaction_result", outboxEvents[0].EventType)

	var resultEvent events.TransactionResult
	err = json.Unmarshal(outboxEvents[0].RawPayload, &resultEvent)
	require.NoError(t, err)
	assert.Equal(t, "failed", resultEvent.Status)
	assert.Contains(t, resultEvent.FailureReason, "nonexistent account id")
}

func TestTransactionCreatedHandler_HandleEvent_InvalidJSON(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(nil)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	ctx := context.Background()

	invalidJSON := []byte(`{"invalid": "json" missing closing brace`)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = handler.HandleEvent(ctx, tx, invalidJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transaction created")
}

func TestTransactionCreatedHandler_GetEventID_Success(t *testing.T) {
	t.Parallel()

	balanceRepo := postgres.NewBalanceRepository(nil)
	accountRepo := postgres.NewAccountRepository(nil)
	outboxRepo := outbox.NewPostgresRepository(nil)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	eventData := []byte(`{"event_id": "test-event-123", "transaction_id": "txn-456"}`)

	eventID, err := handler.GetEventID(eventData)
	require.NoError(t, err)
	assert.Equal(t, "test-event-123", eventID)
}

func TestTransactionCreatedHandler_GetEventID_InvalidJSON(t *testing.T) {
	t.Parallel()

	balanceRepo := postgres.NewBalanceRepository(nil)
	accountRepo := postgres.NewAccountRepository(nil)
	outboxRepo := outbox.NewPostgresRepository(nil)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	handler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	invalidJSON := []byte(`{"invalid": "json"`)

	_, err := handler.GetEventID(invalidJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to extract event ID")
}
