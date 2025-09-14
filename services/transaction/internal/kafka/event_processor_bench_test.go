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
)

func BenchmarkEventProcessor_HandleTransactionResultEvent(b *testing.B) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

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

	transactionIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		transactionID := uuid.New().String()
		transactionIDs[i] = transactionID
		fromAccountID := uuid.New().String()
		toAccountID := uuid.New().String()
		setupBenchmarkTransaction(b, pool, transactionID, fromAccountID, toAccountID, float64(i%1000+100), "pending")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := events.TransactionResult{
			EventID:       "bench_result_" + uuid.New().String(),
			TransactionID: transactionIDs[i],
			Status:        "completed",
			EventType:     "transaction_result",
			Timestamp:     time.Now(),
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			b.Fatalf("Failed to marshal event: %v", err)
		}

		kafkaMessage := gokafka.Message{
			Value: eventBytes,
			Topic: "transactions.results",
			Time:  time.Now(),
		}

		err = processor.HandleTransactionResultEvent(ctx, kafkaMessage)
		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}
