package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/config"
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

func setupTestPostgres(t *testing.T) *pgxpool.Pool {
	var cfg config.TestConfig
	err := config.LoadTest(&cfg)
	require.NoError(t, err)

	pool, err := pgxpool.New(context.Background(), cfg.PostgresTest.ConnectionString())
	require.NoError(t, err)

	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(pool, migrationPath)
	require.NoError(t, err)

	return pool
}

func dropTestPostgres(t *testing.T, pool *pgxpool.Pool) {
	migrationPath := "../migrations"
	err := postgresshared.MigrateDown(pool, migrationPath)
	require.NoError(t, err)
}

func setupTestAccount(t *testing.T, pool *pgxpool.Pool, accountID, userID string, initialBalance int64) {
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into accounts (account_id, user_id, currency, created_at, updated_at)
		values ($1, $2, 'USD', now(), now())
	`, accountID, userID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		insert into balances (account_id, amount, last_updated)
		values ($1, $2, now())
	`, accountID, initialBalance)
	require.NoError(t, err)
}

func BenchmarkEventProcessor_HandleTransactionEvent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	setupBenchmarkAccount(b, pool, fromAccountID, userID, 1000000)
	setupBenchmarkAccount(b, pool, toAccountID, userID, 500000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := events.TransactionCreated{
			EventID:       "bench_event_" + uuid.New().String(),
			TransactionID: "bench_txn_" + uuid.New().String(),
			Type:          "transfer",
			Amount:        float64(i%1000 + 100),
			Currency:      "USD",
			FromAccountID: fromAccountID,
			ToAccountID:   toAccountID,
			Timestamp:     time.Now(),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.created",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventProcessor_HandleTransactionEvent_Deposits(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	setupBenchmarkAccount(b, pool, accountID, userID, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := events.TransactionCreated{
			EventID:       "bench_deposit_" + uuid.New().String(),
			TransactionID: "bench_dep_txn_" + uuid.New().String(),
			Type:          "deposit",
			Amount:        float64(i%500 + 50),
			Currency:      "USD",
			ToAccountID:   accountID,
			Timestamp:     time.Now(),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.created",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventProcessor_HandleTransactionEvent_Withdrawals(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	setupBenchmarkAccount(b, pool, accountID, userID, 500000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := events.TransactionCreated{
			EventID:       "bench_withdrawal_" + uuid.New().String(),
			TransactionID: "bench_with_txn_" + uuid.New().String(),
			Type:          "withdrawal",
			Amount:        float64(i%200 + 10),
			Currency:      "USD",
			FromAccountID: accountID,
			Timestamp:     time.Now(),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.created",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventProcessor_HandleTransactionEvent_Transfers(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	accountPairs := make([]struct {
		fromAccountID string
		toAccountID   string
	}, 5)

	for i := 0; i < 5; i++ {
		fromAccountID := uuid.New().String()
		toAccountID := uuid.New().String()
		userID := uuid.New().String()

		setupBenchmarkAccount(b, pool, fromAccountID, userID, 1000000)
		setupBenchmarkAccount(b, pool, toAccountID, userID, 500000)

		accountPairs[i] = struct {
			fromAccountID string
			toAccountID   string
		}{fromAccountID, toAccountID}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pair := accountPairs[i%len(accountPairs)]

		event := events.TransactionCreated{
			EventID:       "bench_transfer_" + uuid.New().String(),
			TransactionID: "bench_transfer_txn_" + uuid.New().String(),
			Type:          "transfer",
			Amount:        float64(i%500 + 50),
			Currency:      "USD",
			FromAccountID: pair.fromAccountID,
			ToAccountID:   pair.toAccountID,
			Timestamp:     time.Now(),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.created",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventProcessor_HandleTransactionEvent_MixedOperations(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	accounts := make([]string, 10)
	for i := 0; i < 10; i++ {
		accountID := uuid.New().String()
		accounts[i] = accountID
		setupBenchmarkAccount(b, pool, accountID, uuid.New().String(), 1000000)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		accountIndex := i % len(accounts)
		accountID := accounts[accountIndex]

		var event events.TransactionCreated

		switch i % 3 {
		case 0:
			event = events.TransactionCreated{
				EventID:       "bench_mixed_deposit_" + uuid.New().String(),
				TransactionID: "bench_mixed_dep_txn_" + uuid.New().String(),
				Type:          "deposit",
				Amount:        float64(i%300 + 100),
				Currency:      "USD",
				ToAccountID:   accountID,
				Timestamp:     time.Now(),
			}
		case 1:
			event = events.TransactionCreated{
				EventID:       "bench_mixed_withdrawal_" + uuid.New().String(),
				TransactionID: "bench_mixed_with_txn_" + uuid.New().String(),
				Type:          "withdrawal",
				Amount:        float64(i%200 + 50),
				Currency:      "USD",
				FromAccountID: accountID,
				Timestamp:     time.Now(),
			}
		case 2:
			toAccountID := accounts[(accountIndex+1)%len(accounts)]
			event = events.TransactionCreated{
				EventID:       "bench_mixed_transfer_" + uuid.New().String(),
				TransactionID: "bench_mixed_transfer_txn_" + uuid.New().String(),
				Type:          "transfer",
				Amount:        float64(i%400 + 75),
				Currency:      "USD",
				FromAccountID: accountID,
				ToAccountID:   toAccountID,
				Timestamp:     time.Now(),
			}
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.created",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventProcessor_HandleTransactionEvent_Concurrent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	accounts := make([]string, 10)
	for i := 0; i < 10; i++ {
		accountID := uuid.New().String()
		accounts[i] = accountID
		setupBenchmarkAccount(b, pool, accountID, uuid.New().String(), 100000)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fromAccount := accounts[i%len(accounts)]
			toAccount := accounts[(i+1)%len(accounts)]

			event := events.TransactionCreated{
				EventID:       "bench_conc_" + uuid.New().String(),
				TransactionID: "bench_conc_txn_" + uuid.New().String(),
				Type:          "transfer",
				Amount:        float64(i%100 + 10),
				Currency:      "USD",
				FromAccountID: fromAccount,
				ToAccountID:   toAccount,
				Timestamp:     time.Now(),
			}

			eventBytes, err := json.Marshal(event)
			if err != nil {
				b.Fatalf("Failed to marshal event: %v", err)
			}

			kafkaMessage := gokafka.Message{
				Value: eventBytes,
				Topic: "transactions.created",
				Time:  time.Now(),
			}

			err = processor.HandleTransactionCreatedEvent(ctx, kafkaMessage)
			if err != nil {
				b.Fatalf("Failed to process event: %v", err)
			}
			i++
		}
	})
}

func setupBenchmarkPostgres(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}) *pgxpool.Pool {
	t.Helper()

	var cfg config.TestConfig
	err := config.LoadTest(&cfg)
	if err != nil {
		t.Errorf("Failed to load test config: %v", err)
		t.FailNow()
	}

	pool, err := pgxpool.New(context.Background(), cfg.PostgresTest.ConnectionString())
	if err != nil {
		t.Errorf("Failed to connect to test database: %v", err)
		t.FailNow()
	}

	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(pool, migrationPath)
	if err != nil {
		t.Errorf("Failed to run up migrations: %v", err)
		t.FailNow()
	}

	return pool
}

func dropBenchmarkPostgres(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}, pool *pgxpool.Pool) {
	t.Helper()

	migrationPath := "../migrations"
	err := postgresshared.MigrateDown(pool, migrationPath)
	if err != nil {
		t.Errorf("Failed to run down migrations: %v", err)
		t.FailNow()
	}
}

func setupBenchmarkAccount(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}, pool *pgxpool.Pool, accountID, userID string, initialBalance int64) {
	t.Helper()

	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into accounts (account_id, user_id, currency, created_at, updated_at)
		values ($1, $2, 'USD', now(), now())
	`, accountID, userID)
	if err != nil {
		t.Errorf("Failed to insert test account: %v", err)
		t.FailNow()
	}

	_, err = pool.Exec(ctx, `
		insert into balances (account_id, amount, last_updated)
		values ($1, $2, now())
	`, accountID, initialBalance)
	if err != nil {
		t.Errorf("Failed to insert test balance: %v", err)
		t.FailNow()
	}
}
