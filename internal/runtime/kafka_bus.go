package runtime

import (
	"io"

	"github.com/simpletrack/analytics-core/eventbus"
	kafkaeventbus "github.com/simpletrack/analytics-core/eventbus/kafka"
	"github.com/simpletrack/analytics-service/internal/config"
)

// newKafkaBus builds the production EventBus provider.
func newKafkaBus(cfg config.Config) (eventbus.EventBus, []io.Closer, error) {
	// Keep Sarama and Kafka provider details behind analytics-core so the
	// service runtime only maps operator-facing env into provider options.
	bus, err := kafkaeventbus.New(kafkaeventbus.Options{
		Brokers:         cfg.KafkaBrokers,
		Topic:           cfg.KafkaTopic,
		DeadLetterTopic: cfg.KafkaDeadLetterTopic,
		ClientID:        cfg.KafkaClientID,
		MaxAttempts:     cfg.KafkaMaxAttempts,
		RetryBackoff:    cfg.KafkaRetryBackoff,
		Workers:         cfg.KafkaWorkers,
		QueueSize:       cfg.KafkaQueueSize,
		CommitInterval:  cfg.KafkaCommitInterval,
	})
	if err != nil {
		return nil, nil, err
	}
	return bus, []io.Closer{bus}, nil
}
