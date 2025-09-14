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
)

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
