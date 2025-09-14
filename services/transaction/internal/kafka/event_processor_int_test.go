package kafka_test

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventProcessor_HandleTransactionResultEvent_Success(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		transactionRepo,
		eventTrackingRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	transactionID := uuid.New().String()
	fromAccountID := uuid.New().String()
	toAccountID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, fromAccountID, toAccountID, 100.0, "pending")

	event := events.TransactionResult{
		EventID:       "test_result_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "completed",
		EventType:     "transaction_result",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.results",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	updatedTransaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "completed", string(updatedTransaction.Status))

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestEventProcessor_HandleTransactionResultEvent_Failed(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		transactionRepo,
		eventTrackingRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	transactionID := uuid.New().String()
	fromAccountID := uuid.New().String()
	toAccountID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, fromAccountID, toAccountID, 100.0, "pending")

	failureReason := "insufficient funds"
	event := events.TransactionResult{
		EventID:       "test_failed_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "failed",
		FailureReason: failureReason,
		EventType:     "transaction_result",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.results",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	updatedTransaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "failed", string(updatedTransaction.Status))
	assert.Equal(t, failureReason, *updatedTransaction.ErrorMessage)

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestEventProcessor_HandleTransactionResultEvent_Idempotency(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		transactionRepo,
		eventTrackingRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	transactionID := uuid.New().String()
	fromAccountID := uuid.New().String()
	toAccountID := uuid.New().String()
	setupTestTransaction(t, pool, transactionID, fromAccountID, toAccountID, 100.0, "pending")

	event := events.TransactionResult{
		EventID:       "idempotent_test_" + uuid.New().String(),
		TransactionID: transactionID,
		Status:        "completed",
		EventType:     "transaction_result",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.results",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	updatedTransaction, err := transactionRepo.GetTransaction(ctx, transactionID)
	require.NoError(t, err)
	assert.Equal(t, "completed", string(updatedTransaction.Status))

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestEventProcessor_HandleTransactionResultEvent_TransactionNotFound(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		transactionRepo,
		eventTrackingRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	nonExistentTransactionID := uuid.New().String()
	event := events.TransactionResult{
		EventID:       "not_found_test_" + uuid.New().String(),
		TransactionID: nonExistentTransactionID,
		Status:        "completed",
		EventType:     "transaction_result",
		Timestamp:     time.Now(),
	}

	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	kafkaMessage := gokafka.Message{
		Value: eventBytes,
		Topic: "transactions.results",
		Time:  time.Now(),
	}

	err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.NoError(t, err)

	processed, err := eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestEventProcessor_HandleTransactionResultEvent_InvalidJSON(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	processor := kafka.NewEventProcessor(
		transactionRepo,
		eventTrackingRepo,
		db,
		transactionService,
	)

	ctx := context.Background()

	invalidJSON := []byte(`{"invalid": "json"`)
	kafkaMessage := gokafka.Message{
		Value: invalidJSON,
		Topic: "transactions.results",
		Time:  time.Now(),
	}

	err := processor.HandleTransactionResultEvent(ctx, kafkaMessage)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal")
}
