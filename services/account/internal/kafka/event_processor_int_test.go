package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventProcessor_HandleTransactionEvent_Success(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	fromAccountID := uuid.New().String()
	toAccountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(10000)
	transferAmount := 2500.00

	setupTestAccount(t, pool, fromAccountID, userID, initialBalance)
	setupTestAccount(t, pool, toAccountID, userID, 5000)

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
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.created",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	fromBalance, err := balanceRepo.GetBalance(ctx, fromAccountID)
	require.NoError(t, err)
	assert.Equal(t, 7500.00, fromBalance.Amount)

	toBalance, err := balanceRepo.GetBalance(ctx, toAccountID)
	require.NoError(t, err)
	assert.Equal(t, 7500.00, toBalance.Amount)

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)

	outboxEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, outboxEvents, 3)
}

func TestEventProcessor_HandleTransactionEvent_Idempotency(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(10000)
	depositAmount := 1000.00

	setupTestAccount(t, pool, accountID, userID, initialBalance)

	event := events.TransactionCreated{
		EventID:       "idempotent_test_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "deposit",
		Amount:        depositAmount,
		Currency:      "USD",
		ToAccountID:   accountID,
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.created",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	balanceAfterFirst, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	expectedBalance := float64(initialBalance) + depositAmount
	assert.Equal(t, expectedBalance, balanceAfterFirst.Amount)

	err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	balanceAfterSecond, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, expectedBalance, balanceAfterSecond.Amount)
	assert.Equal(t, balanceAfterFirst.Amount, balanceAfterSecond.Amount)

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestEventProcessor_HandleTransactionEvent_AccountNotFound(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	nonExistentAccountID := uuid.New().String()
	event := events.TransactionCreated{
		EventID:       "nonexistent_account_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "deposit",
		Amount:        1000.00,
		Currency:      "USD",
		ToAccountID:   nonExistentAccountID,
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.created",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestEventProcessor_HandleTransactionEvent_InsufficientFunds(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	accountID := uuid.New().String()
	userID := uuid.New().String()
	initialBalance := int64(500)
	withdrawalAmount := 1000.00

	setupTestAccount(t, pool, accountID, userID, initialBalance)

	event := events.TransactionCreated{
		EventID:       "insufficient_funds_" + uuid.New().String(),
		TransactionID: "txn_" + uuid.New().String(),
		Type:          "withdrawal",
		Amount:        withdrawalAmount,
		Currency:      "USD",
		FromAccountID: accountID,
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.created",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient funds")

	balance, err := balanceRepo.GetBalance(ctx, accountID)
	require.NoError(t, err)
	assert.Equal(t, float64(initialBalance), balance.Amount)

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestEventProcessor_HandleTransactionEvent_InvalidJSON(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	invalidJSON := []byte(`{"invalid": "json" missing closing brace`)
	kafkaMessage := gokafka.Message{
		Value: invalidJSON,
		Topic: "transactions.created",
		Time:  time.Now(),
	}

	err := processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transaction event")
}
