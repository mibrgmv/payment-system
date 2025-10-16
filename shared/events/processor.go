package events

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-system/shared/inbox"
	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/postgres"
	kafkago "github.com/segmentio/kafka-go"
)

type ProcessorConfig struct {
	ServiceName     string
	KafkaBrokers    []string
	TopicHandlers   map[string]string // topic -> eventType mapping
	ConsumerConfigs map[string]kafka.ConsumerConfig
}

type Processor struct {
	registry  *ProcessorRegistry
	inboxRepo inbox.Repository
	db        *postgres.DB
	config    ProcessorConfig
}

func NewProcessor(
	registry *ProcessorRegistry,
	inboxRepo inbox.Repository,
	db *postgres.DB,
	config ProcessorConfig,
) *Processor {
	return &Processor{
		registry:  registry,
		inboxRepo: inboxRepo,
		db:        db,
		config:    config,
	}
}

func (p *Processor) Start(ctx context.Context) error {
	for topic, eventType := range p.config.TopicHandlers {
		handler, exists := p.registry.GetHandler(eventType)
		if !exists {
			return fmt.Errorf("no handler registered for event type: %s", eventType)
		}

		consumerConfig, exists := p.config.ConsumerConfigs[topic]
		if !exists {
			return fmt.Errorf("no consumer config registered for topic: %s", eventType)
		}

		consumer := kafka.NewConsumer(consumerConfig, func(ctx context.Context, message kafkago.Message) error {
			return p.Process(ctx, message.Value, handler)
		})

		go consumer.Start(ctx)
	}

	return nil
}

func (p *Processor) Process(
	ctx context.Context,
	eventData []byte,
	eventHandler ProcessorEventHandler,
) error {
	eventID, err := eventHandler.GetEventID(eventData)
	if err != nil {
		return fmt.Errorf("failed to get event ID: %w", err)
	}

	processed, err := p.inboxRepo.IsEventProcessed(ctx, eventID)
	if err != nil {
		return fmt.Errorf("failed to check event processing status: %w", err)
	}
	if processed {
		log.Printf("Event %s already processed, skipping", eventID)
		return nil
	}

	err = p.db.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := p.inboxRepo.MarkEventProcessedTx(ctx, tx, eventID, eventHandler.GetEventType()); err != nil {
			if errors.Is(err, inbox.ErrEventAlreadyProcessed) {
				log.Printf("event %s already processed concurrently, skipping", eventID)
				return nil
			}
			return fmt.Errorf("failed to mark event as processed: %w", err)
		}

		if err := eventHandler.HandleEvent(ctx, tx, eventData); err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("event processing failed: %w", err)
	}

	log.Printf("Successfully processed event %s", eventID)
	return nil
}
