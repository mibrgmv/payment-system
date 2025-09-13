package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/postgres"
)

type EventPublisher struct {
	outboxRepo    repository.OutboxRepository
	kafkaProducer *kafka.Producer
	db            *postgres.DB
}

func NewEventPublisher(
	outboxRepo repository.OutboxRepository,
	kafkaProducer *kafka.Producer,
	db *postgres.DB,
) *EventPublisher {
	return &EventPublisher{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		db:            db,
	}
}

func (p *EventPublisher) Start(ctx context.Context) {
	go p.publishLoop(ctx)
	go p.cleanupLoop(ctx)
}

func (p *EventPublisher) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.ProcessOutboxBatch(ctx); err != nil {
				log.Printf("Error processing outbox batch: %v", err)
			}
		}
	}
}

func (p *EventPublisher) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.outboxRepo.CleanupOldEvents(ctx, 7); err != nil {
				log.Printf("Error cleaning up old events: %v", err)
			}
		}
	}
}

func (p *EventPublisher) ProcessOutboxBatch(ctx context.Context) error {
	pending, err := p.outboxRepo.GetPendingEvents(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}

	if len(pending) == 0 {
		return nil
	}

	for _, event := range pending {
		if err := p.ProcessSingleEvent(ctx, event); err != nil {
			log.Printf("Failed to process event %s: %v", event.EventID, err)
		}
	}

	log.Printf("Processed %d events from outbox", len(pending))
	return nil
}

func (p *EventPublisher) ProcessSingleEvent(ctx context.Context, event events.OutboxEvent) error {
	err := p.db.WithTransaction(ctx, func(tx pgx.Tx) error {
		lockedEvent, err := p.outboxRepo.LockEventForProcessing(ctx, tx, event.EventID)
		if err != nil {
			return fmt.Errorf("failed to lock event: %w", err)
		}

		if lockedEvent == nil {
			return nil
		}

		if err := p.publishEvent(ctx, *lockedEvent); err != nil {
			if markErr := p.outboxRepo.MarkEventAsFailedTx(ctx, tx, event.EventID, err.Error()); markErr != nil {
				return fmt.Errorf("failed to mark event as failed: %w", markErr)
			}
			return err
		}

		return p.outboxRepo.MarkEventAsPublishedTx(ctx, tx, event.EventID)
	})

	if err != nil {
		return fmt.Errorf("failed to process event %s: %w", event.EventID, err)
	}

	return nil
}

func (p *EventPublisher) publishEvent(ctx context.Context, event events.OutboxEvent) error {
	switch event.EventType {
	case "balance_updated":
		var payload events.BalanceUpdated
		if err := event.UnmarshalPayload(&payload); err != nil {
			return fmt.Errorf("failed to unmarshal balance updated event: %w", err)
		}
		return p.kafkaProducer.Produce(ctx, event.Topic, event.EventID, payload)

	case "transaction_result":
		var payload events.TransactionResult
		if err := event.UnmarshalPayload(&payload); err != nil {
			return fmt.Errorf("failed to unmarshal transaction result event: %w", err)
		}
		return p.kafkaProducer.Produce(ctx, event.Topic, event.EventID, payload)

	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}
