package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Produce(ctx context.Context, topic string, key string, value interface{}) error
	Close() error
}

type ProducerConfig struct {
	Brokers  []string
	ClientID string
}

type producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg ProducerConfig) Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
		Async:        true,
		Logger:       kafka.LoggerFunc(log.Printf),
		ErrorLogger:  kafka.LoggerFunc(log.Printf),
	}

	return &producer{writer: writer}
}

func (p *producer) Produce(ctx context.Context, topic string, key string, value interface{}) error {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: valueBytes,
		Time:  time.Now(),
	}

	return p.writer.WriteMessages(ctx, message)
}

func (p *producer) Close() error {
	return p.writer.Close()
}
