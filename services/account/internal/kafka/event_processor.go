package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/shared/events/event_tracking"
	"github.com/mibrgmv/payment-service/shared/json"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
	"github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/segmentio/kafka-go"
)

type EventProcessor struct {
	balanceRepo        repository.BalanceRepository
	accountRepo        repository.AccountRepository
	eventTrackingRepo  event_tracking.Repository
	outboxRepo         outbox.Repository
	db                 *postgres.DB
	transactionService service.TransactionService
}

func NewEventProcessor(
	balanceRepo repository.BalanceRepository,
	accountRepo repository.AccountRepository,
	eventTrackingRepo event_tracking.Repository,
	outboxRepo outbox.Repository,
	db *postgres.DB,
	transactionService service.TransactionService,
) *EventProcessor {
	return &EventProcessor{
		balanceRepo:        balanceRepo,
		accountRepo:        accountRepo,
		eventTrackingRepo:  eventTrackingRepo,
		outboxRepo:         outboxRepo,
		db:                 db,
		transactionService: transactionService,
	}
}

func (c *EventProcessor) StartConsumers(ctx context.Context, kafkaBrokers []string) {
	transactionConfig := kafkashared.ConsumerConfig{
		Brokers:         kafkaBrokers,
		GroupID:         "account-service-transactions",
		Topic:           "transactions.created",
		AutoOffsetReset: "latest",
		MaxWait:         1 * time.Second,
		MinBytes:        10,
		MaxBytes:        10e6, // 10MB
	}

	transactionConsumer := kafkashared.NewConsumer(transactionConfig, c.HandleTransactionCreatedEvent)
	go transactionConsumer.Start(ctx)
}

func (c *EventProcessor) HandleTransactionCreatedEvent(ctx context.Context, message kafka.Message) error {
	var event events.TransactionCreated
	if err := json.StrictUnmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal transaction event: %w", err)
	}

	processed, err := c.eventTrackingRepo.IsEventProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("failed to check event processing status: %w", err)
	}
	if processed {
		log.Printf("Event %s already processed, skipping", event.EventID)
		return nil
	}

	err = c.db.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := c.eventTrackingRepo.MarkEventProcessedTx(ctx, tx, event.EventID, event.Type); err != nil {
			if errors.Is(err, event_tracking.ErrEventAlreadyProcessed) {
				log.Printf("event %s already processed concurrently, skipping", event.EventID)
				return nil
			}
			return fmt.Errorf("failed to mark event as processed: %w", err)
		}

		if err := c.transactionService.HandleTransactionCreated(ctx, tx, event); err != nil {
			return fmt.Errorf("failed to process transaction: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("transaction processing failed: %w", err)
	}

	log.Printf("Successfully processed transaction event %s", event.EventID)
	return nil
}
