package server

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/events"
	"github.com/mibrgmv/go-platform/inbox"
	platformkafka "github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/outbox"
	platformpostgres "github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/payment-system/transaction/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-system/transaction/internal/kafka/producer_handlers"
	"github.com/mibrgmv/payment-system/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/transaction/internal/service"
)

func SetupKafkaProcessor(pool *pgxpool.Pool, kafkaCfg platformkafka.Config) *events.Processor {
	transactionRepo := postgres.NewTransactionRepository(pool)
	inboxRepo := inbox.NewPostgresRepository(pool, "transaction-service")
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := platformpostgres.NewDB(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	transactionResultHandler := consumer_handlers.NewTransactionResultHandler(transactionService)

	registry := events.NewConsumerRegistry()
	registry.Register(transactionResultHandler)

	config := events.ProcessorConfig{
		ServiceName:  "transaction-service",
		KafkaBrokers: kafkaCfg.Brokers,
		TopicHandlers: map[string]string{
			"transactions.results": "transaction_result",
		},
		ConsumerConfigs: map[string]platformkafka.ConsumerConfig{
			"transactions.results": {
				Brokers:         kafkaCfg.Brokers,
				GroupID:         "transaction-service-results",
				Topic:           "transactions.results",
				AutoOffsetReset: "latest",
				MaxWait:         1 * time.Second,
				MinBytes:        10,
				MaxBytes:        10e6,
			},
		},
	}

	return events.NewProcessor(registry, inboxRepo, db, config)
}

func SetupKafkaPublisher(pool *pgxpool.Pool, kafkaCfg platformkafka.Config) *events.Publisher {
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := platformpostgres.NewDB(pool)
	producer := platformkafka.NewProducer(platformkafka.ProducerConfig{
		Brokers:  kafkaCfg.Brokers,
		ClientID: "transaction-service-producer",
	})

	publisherConfig := events.PublisherConfig{
		BatchSize:         100,
		WorkerCount:       10,
		ProcessInterval:   5 * time.Second,
		RetryInterval:     10 * time.Second,
		CleanupInterval:   1 * time.Hour,
		CleanupDays:       7,
		BaseRetryDelay:    100 * time.Millisecond,
		MaxRetryBackoff:   5 * time.Second,
		RetryJitterFactor: 0.2,
	}

	registry := events.NewPublisherRegistry()
	registry.Register(producer_handlers.NewTransactionCreatedHandler())

	return events.NewPublisher(registry, outboxRepo, producer, db, publisherConfig)
}
