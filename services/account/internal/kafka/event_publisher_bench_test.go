package kafka_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
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
		setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")

		err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
			EventID:   eventID,
			EventType: "balance_updated",
			Topic:     "balances.updated",
		})

		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventPublisher_ProcessOutboxBatch(b *testing.B) {
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
			setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")
		}

		err := publisher.ProcessOutboxBatch(ctx)
		if err != nil {
			b.Fatalf("Failed to process batch: %v", err)
		}
	}
}

func BenchmarkEventPublisher_ProcessOutboxBatch_Concurrent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			eventID := uuid.New().String()
			setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")

			err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
				EventID:   eventID,
				EventType: "balance_updated",
				Topic:     "balances.updated",
			})

			if err != nil {
				b.Fatalf("Failed to process event: %v", err)
			}
			i++
		}
	})
}
