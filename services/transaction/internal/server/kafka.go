package server

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-system/services/transaction/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-system/services/transaction/internal/kafka/producer_handlers"
	"github.com/mibrgmv/payment-system/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/services/transaction/internal/service"
	"github.com/mibrgmv/payment-system/shared/events"
	"github.com/mibrgmv/payment-system/shared/inbox"
	sharedkafka "github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/outbox"
	sharedpostgres "github.com/mibrgmv/payment-system/shared/postgres"
)

func SetupKafkaProcessor(pool *pgxpool.Pool, kafkaCfg sharedkafka.Config) *events.Processor {
	transactionRepo := postgres.NewTransactionRepository(pool)
	inboxRepo := inbox.NewPostgresRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := sharedpostgres.NewDB(pool)
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
		ConsumerConfigs: map[string]sharedkafka.ConsumerConfig{
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

func SetupKafkaPublisher(pool *pgxpool.Pool, kafkaCfg sharedkafka.Config) *events.Publisher {
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := sharedpostgres.NewDB(pool)
	producer := sharedkafka.NewProducer(sharedkafka.ProducerConfig{
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
