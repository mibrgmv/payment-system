package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	"github.com/mibrgmv/payment-service/shared/events"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
)

func SetupKafkaProcessor(pool *pgxpool.Pool) *kafka.EventProcessor {
	transactionRepo := postgres.NewTransactionRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionEventHandler := service.NewTransactionEventHandler(transactionRepo, outboxRepo)

	return kafka.NewEventProcessor(
		eventTrackingRepo,
		db,
		transactionEventHandler,
	)
}

func SetupKafkaPublisher(pool *pgxpool.Pool, kafkaCfg kafkashared.Config) *events.Publisher {
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgresshared.NewDB(pool)
	producer := kafkashared.NewProducer(kafkashared.ProducerConfig{
		Brokers:  kafkaCfg.Brokers,
		ClientID: "transaction-service-producer",
	})
	eventHandler := kafka.NewTransactionEventHandler()

	return events.NewPublisher(
		outboxRepo,
		producer,
		db,
		eventHandler,
	)
}
