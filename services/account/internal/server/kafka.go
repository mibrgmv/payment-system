package server

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/events"
	"github.com/mibrgmv/go-platform/inbox"
	platformkafka "github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/outbox"
	platformpostgres "github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/payment-system/account/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-system/account/internal/kafka/producer_handlers"
	"github.com/mibrgmv/payment-system/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/account/internal/service"
)

func SetupKafkaProcessor(pool *pgxpool.Pool, kafkaCfg platformkafka.Config) *events.Processor {
	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	inboxRepo := inbox.NewPostgresRepository(pool, "account-service")
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := platformpostgres.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	transactionCreatedHandler := consumer_handlers.NewTransactionCreatedHandler(transactionService)

	registry := events.NewConsumerRegistry()
	registry.Register(transactionCreatedHandler)

	config := events.ProcessorConfig{
		ServiceName:  "account-service",
		KafkaBrokers: kafkaCfg.Brokers,
		TopicHandlers: map[string]string{
			"transactions.created": "transaction_created",
		},
		ConsumerConfigs: map[string]platformkafka.ConsumerConfig{
			"transactions.created": {
				Brokers:         kafkaCfg.Brokers,
				GroupID:         "account-service-transactions",
				Topic:           "transactions.created",
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
		ClientID: "account-service-producer",
	})

	registry := events.NewPublisherRegistry()
	registry.Register(producer_handlers.NewTransactionResultHandler())

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

	return events.NewPublisher(registry, outboxRepo, producer, db, publisherConfig)
}
