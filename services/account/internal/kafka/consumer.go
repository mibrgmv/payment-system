package kafka

import (
	"context"
	"log"
	"time"

	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/segmentio/kafka-go"
)

type BalanceChangeConsumer struct {
	reader    *kafka.Reader
	processor *service.BalanceProcessor
}

func NewBalanceChangeConsumer(brokers []string, topic string, groupID string, processor *service.BalanceProcessor) *BalanceChangeConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &BalanceChangeConsumer{
		reader:    reader,
		processor: processor,
	}
}

func (c *BalanceChangeConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
			message, err := c.reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading kafka message: %v", err)
				continue
			}

			if err := c.processor.ProcessBalanceChangeEvent(ctx, message.Value); err != nil {
				log.Printf("Error processing balance change event: %v", err)
				// Depending on your error handling strategy, you might want to:
				// - Continue processing other messages
				// - Send to dead letter queue
				// - Retry with exponential backoff
				continue
			}

			log.Printf("Successfully processed message from offset %d", message.Offset)
		}
	}
}
