package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/shared/events"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
)

func SetupKafkaProcessor(pool *pgxpool.Pool) *kafka.EventProcessor {
	balanceRepo := postgres.NewBalanceRepository(pool)
	accountRepo := postgres.NewAccountRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgresshared.NewDB(pool)
	transactionService := service.NewTransactionService(balanceRepo, accountRepo, outboxRepo)

	return kafka.NewEventProcessor(
		balanceRepo,
		accountRepo,
		eventTrackingRepo,
		outboxRepo,
		db,
		transactionService,
	)
}

func SetupKafkaPublisher(pool *pgxpool.Pool, kafkaCfg kafkashared.Config) *events.Publisher {
	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgresshared.NewDB(pool)
	producer := kafkashared.NewProducer(kafkashared.ProducerConfig{
		Brokers:  kafkaCfg.Brokers,
		ClientID: "account-service-producer",
	})
	eventHandler := kafka.NewAccountEventHandler()

	return events.NewPublisher(
		outboxRepo,
		producer,
		db,
		eventHandler,
	)
}
