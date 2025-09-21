package events

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-service/shared/kafka"
	"github.com/mibrgmv/payment-service/shared/outbox"
	"github.com/mibrgmv/payment-service/shared/postgres"
)

type PublisherConfig struct {
	BatchSize       int           `yaml:"batch_size"`
	WorkerCount     int           `yaml:"worker_count"`
	ProcessInterval time.Duration `yaml:"process_interval"`
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
	CleanupDays     int           `yaml:"cleanup_days"`
}

type Publisher struct {
	registry      *PublisherRegistry
	outboxRepo    outbox.Repository
	kafkaProducer kafka.Producer
	db            *postgres.DB
	config        PublisherConfig
}

func NewPublisher(
	registry *PublisherRegistry,
	outboxRepo outbox.Repository,
	kafkaProducer kafka.Producer,
	db *postgres.DB,
	config PublisherConfig,
) *Publisher {
	return &Publisher{
		registry:      registry,
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		db:            db,
		config:        config,
	}
}

func (p *Publisher) Start(ctx context.Context) {
	go p.publishLoop(ctx)
	go p.cleanupLoop(ctx)
}

func (p *Publisher) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.ProcessInterval)
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

func (p *Publisher) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.outboxRepo.CleanupOldEvents(ctx, p.config.CleanupDays); err != nil {
				log.Printf("Error cleaning up old events: %v", err)
			}
		}
	}
}

func (p *Publisher) ProcessOutboxBatch(ctx context.Context) error {
	pending, err := p.outboxRepo.GetPendingEvents(ctx, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	workerCount := min(len(pending), p.config.WorkerCount)
	jobs := make(chan outbox.Event, len(pending))
	results := make(chan error, len(pending))

	for w := 0; w < workerCount; w++ {
		go func() {
			for event := range jobs {
				results <- p.ProcessSingleEvent(ctx, event)
			}
		}()
	}

	for _, event := range pending {
		jobs <- event
	}
	close(jobs)

	successCount := 0
	for i := 0; i < len(pending); i++ {
		if err := <-results; err != nil {
			log.Printf("Failed to process event: %v", err)
		} else {
			successCount++
		}
	}

	//var wg sync.WaitGroup
	//sem := make(chan struct{}, 20)
	//
	//for _, event := range pending {
	//	wg.Add(1)
	//	sem <- struct{}{}
	//	go func(evt events.OutboxEvent) {
	//		defer func() {
	//			<-sem
	//			wg.Done()
	//		}()
	//		if err := p.ProcessSingleEvent(ctx, evt); err != nil {
	//			log.Printf("Failed to process event %s: %v", evt.EventID, err)
	//		}
	//	}(event)
	//}
	//
	//wg.Wait()

	log.Printf("Processed %d events from outbox (%d successful)", len(pending), successCount)
	return nil
}

func (p *Publisher) ProcessSingleEvent(ctx context.Context, event outbox.Event) error {
	err := p.db.WithTransaction(ctx, func(tx pgx.Tx) error {
		lockedEvent, err := p.outboxRepo.LockEventForProcessing(ctx, tx, event.EventID)
		if err != nil {
			return fmt.Errorf("failed to lock event: %w", err)
		}
		if lockedEvent == nil {
			return nil
		}

		handler, exists := p.registry.GetHandler(lockedEvent.EventType)
		if !exists {
			return fmt.Errorf("no handler registered for event type: %s", lockedEvent.EventType)
		}

		if err := handler.HandleEvent(ctx, lockedEvent, p.kafkaProducer); err != nil {
			if markErr := p.outboxRepo.MarkEventAsFailedTx(ctx, tx, event.EventID, err.Error()); markErr != nil {
				return fmt.Errorf("failed to mark event as failed: %w", markErr)
			}
			return nil
		}

		return p.outboxRepo.MarkEventAsPublishedTx(ctx, tx, event.EventID)
	})

	if err != nil {
		return fmt.Errorf("failed to process event %s: %w", event.EventID, err)
	}

	return nil
}
