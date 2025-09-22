package server

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/consumer_handlers"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/producer_handlers"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/shared/events"
	"github.com/mibrgmv/payment-service/shared/inbox"
	sharedkafka "github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
	sharedpostgres "github.com/mibrgmv/payment-service/shared/postgres"
)

func SetupKafkaProcessor(pool *pgxpool.Pool, kafkaCfg sharedkafka.Config) *events.Processor {
	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	inboxRepo := inbox.NewPostgresRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := sharedpostgres.NewDB(pool)
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
		ConsumerConfigs: map[string]sharedkafka.ConsumerConfig{
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

func SetupKafkaPublisher(pool *pgxpool.Pool, kafkaCfg sharedkafka.Config) *events.Publisher {
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := sharedpostgres.NewDB(pool)
	producer := sharedkafka.NewProducer(sharedkafka.ProducerConfig{
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
