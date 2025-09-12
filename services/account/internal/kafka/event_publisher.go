package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mibrgmv/payment-service/services/account/internal/kafka/models"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/shared/kafka"
)

type EventPublisher struct {
	outboxRepo    repository.OutboxRepository
	kafkaProducer *kafka.Producer
}

func NewEventPublisher(outboxRepo repository.OutboxRepository, kafkaProducer *kafka.Producer) *EventPublisher {
	return &EventPublisher{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
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
			if err := p.processOutboxBatch(ctx); err != nil {
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

func (p *EventPublisher) processOutboxBatch(ctx context.Context) error {
	tx, err := p.outboxRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	events, err := p.outboxRepo.GetPendingEventsForUpdateTx(ctx, tx, 100)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	// Process each event
	var processedEvents []string
	var failedEvents []struct {
		eventID string
		err     error
	}

	for _, event := range events {
		if err := p.publishEvent(ctx, event); err != nil {
			failedEvents = append(failedEvents, struct {
				eventID string
				err     error
			}{event.EventID, err})
			log.Printf("Failed to publish event %s: %v", event.EventID, err)
		} else {
			processedEvents = append(processedEvents, event.EventID)
		}
	}

	for _, eventID := range processedEvents {
		if err := p.outboxRepo.MarkEventAsPublishedTx(ctx, tx, eventID); err != nil {
			return fmt.Errorf("failed to mark event %s as published: %w", eventID, err)
		}
	}

	for _, failed := range failedEvents {
		if err := p.outboxRepo.MarkEventAsFailedTx(ctx, tx, failed.eventID, failed.err.Error()); err != nil {
			return fmt.Errorf("failed to mark event %s as failed: %w", failed.eventID, err)
		}
	}

	return tx.Commit(ctx)
}

func (p *EventPublisher) publishEvent(ctx context.Context, event models.OutboxEvent) error {
	switch event.EventType {
	case "account_created":
		var payload models.AccountCreatedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return fmt.Errorf("failed to unmarshal account created event: %w", err)
		}
		return p.kafkaProducer.Produce(ctx, event.Topic, event.EventID, payload)

	case "balance_updated":
		var payload models.BalanceUpdatedEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return fmt.Errorf("failed to unmarshal balance updated event: %w", err)
		}
		return p.kafkaProducer.Produce(ctx, event.Topic, event.EventID, payload)

	case "insufficient_funds":
		var payload models.InsufficientFundsEvent
		if err := event.UnmarshalPayload(&payload); err != nil {
			return fmt.Errorf("failed to unmarshal insufficient funds event: %w", err)
		}
		return p.kafkaProducer.Produce(ctx, event.Topic, event.EventID, payload)

	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}
