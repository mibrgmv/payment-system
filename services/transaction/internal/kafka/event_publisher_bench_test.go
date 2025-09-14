package kafka_test

import (
	"context"
	"io"
	"log"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
)

func BenchmarkEventPublisher_ProcessSingleEvent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		eventID := uuid.New().String()
		setupBenchmarkOutboxEvent(b, pool, eventID, "transaction_created", `{
			"transaction_id": "test_txn_`+eventID+`",
			"type": "transfer",
			"amount": 100.0,
			"currency": "USD",
			"from_account_id": "acc_from",
			"to_account_id": "acc_to",
			"timestamp": "2023-12-07T10:00:00Z"
		}`, "transactions.created")

		err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
			EventID:   eventID,
			EventType: "transaction_created",
			Topic:     "transactions.created",
		})

		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventPublisher_ProcessOutboxBatch(b *testing.B) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stdout)

	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			eventID := uuid.New().String()
			setupBenchmarkOutboxEvent(b, pool, eventID, "transaction_created", `{
				"transaction_id": "test_txn_`+eventID+`",
				"type": "transfer",
				"amount": 100.0,
				"currency": "USD",
				"from_account_id": "acc_from",
				"to_account_id": "acc_to",
				"timestamp": "2023-12-07T10:00:00Z"
			}`, "transactions.created")
		}

		err := publisher.ProcessOutboxBatch(ctx)
		if err != nil {
			b.Fatalf("Failed to process batch: %v", err)
		}
	}
}
