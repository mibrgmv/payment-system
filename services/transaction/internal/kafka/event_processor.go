package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	"github.com/mibrgmv/payment-service/shared/json"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/segmentio/kafka-go"
)

type EventProcessor struct {
	transactionRepo    repository.TransactionRepository
	eventTrackingRepo  repository.EventTrackingRepository
	db                 *postgres.DB
	transactionService service.TransactionService
}

func NewEventProcessor(
	transactionRepo repository.TransactionRepository,
	eventTrackingRepo repository.EventTrackingRepository,
	db *postgres.DB,
	transactionService service.TransactionService,
) *EventProcessor {
	return &EventProcessor{
		transactionRepo:    transactionRepo,
		eventTrackingRepo:  eventTrackingRepo,
		db:                 db,
		transactionService: transactionService,
	}
}

func (c *EventProcessor) StartConsumers(ctx context.Context, kafkaBrokers []string) {
	resultConfig := kafkashared.ConsumerConfig{
		Brokers:         kafkaBrokers,
		GroupID:         "transaction-service-results",
		Topic:           "transactions.results",
		AutoOffsetReset: "latest",
		MaxWait:         1 * time.Second,
		MinBytes:        10,
		MaxBytes:        10e6,
	}

	resultConsumer := kafkashared.NewConsumer(resultConfig, c.HandleTransactionResultEvent)
	resultConsumer.Start(ctx)
}

func (c *EventProcessor) HandleTransactionResultEvent(ctx context.Context, message kafka.Message) error {
	var event events.TransactionResult
	if err := json.StrictUnmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal transaction result event: %w", err)
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
		if err := c.eventTrackingRepo.MarkEventProcessedTx(ctx, tx, event.EventID, event.EventType); err != nil {
			if errors.Is(err, repository.ErrEventAlreadyProcessed) {
				log.Printf("event %s already processed concurrently, skipping", event.EventID)
				return nil
			}
			return fmt.Errorf("failed to mark event as processed: %w", err)
		}

		if err := c.transactionService.HandleTransactionResult(ctx, tx, event); err != nil {
			return fmt.Errorf("failed to handle transaction result: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("transaction result processing failed: %w", err)
	}

	log.Printf("Successfully processed transaction result event %s", event.EventID)
	return nil
}
