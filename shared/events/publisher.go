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

type EventHandler interface {
	HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error
}

type Publisher struct {
	outboxRepo    outbox.Repository
	kafkaProducer kafka.Producer
	db            *postgres.DB
	eventHandler  EventHandler
}

func NewPublisher(
	outboxRepo outbox.Repository,
	kafkaProducer kafka.Producer,
	db *postgres.DB,
	eventHandler EventHandler,
) *Publisher {
	return &Publisher{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		db:            db,
		eventHandler:  eventHandler,
	}
}

func (p *Publisher) Start(ctx context.Context) {
	go p.publishLoop(ctx)
	go p.cleanupLoop(ctx)
}

func (p *Publisher) publishLoop(ctx context.Context) {
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

func (p *Publisher) cleanupLoop(ctx context.Context) {
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

func (p *Publisher) ProcessOutboxBatch(ctx context.Context) error {
	pending, err := p.outboxRepo.GetPendingEvents(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to get pending events: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	workerCount := min(len(pending), 10)
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

	for i := 0; i < len(pending); i++ {
		if err := <-results; err != nil {
			log.Printf("Failed to process event: %v", err)
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

	log.Printf("Processed %d events from outbox", len(pending))
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

		if err := p.eventHandler.HandleEvent(ctx, lockedEvent, p.kafkaProducer); err != nil {
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
