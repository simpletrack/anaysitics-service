package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/simpletrack/analytics-core/eventbus"
	kafkaeventbus "github.com/simpletrack/analytics-core/eventbus/kafka"
	"github.com/simpletrack/analytics-service/internal/config"
)

// newKafkaBus builds the production EventBus provider.
func newKafkaBus(cfg config.Config) (eventbus.EventBus, []io.Closer, error) {
	tlsConfig, err := newKafkaTLSConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	saslHandshake := cfg.KafkaSASLHandshake

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
		TLSEnabled:      cfg.KafkaTLSEnabled,
		TLSConfig:       tlsConfig,
		SASLEnabled:     cfg.KafkaSASLEnabled,
		SASLMechanism:   kafkaeventbus.SASLMechanism(strings.ToLower(cfg.KafkaSASLMechanism)),
		SASLUsername:    cfg.KafkaSASLUsername,
		SASLPassword:    cfg.KafkaSASLPassword,
		SASLHandshake:   &saslHandshake,
	})
	if err != nil {
		return nil, nil, err
	}
	return bus, []io.Closer{bus}, nil
}

// newKafkaTLSConfig builds the optional broker TLS config from service env paths.
func newKafkaTLSConfig(cfg config.Config) (*tls.Config, error) {
	if !cfg.KafkaTLSEnabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		ServerName:         cfg.KafkaTLSServerName,
		InsecureSkipVerify: cfg.KafkaTLSInsecureSkipVerify, //nolint:gosec // explicit operator test knob; production runbook forbids enabling it.
		MinVersion:         tls.VersionTLS12,
	}
	if cfg.KafkaTLSCAFile != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		pem, err := os.ReadFile(cfg.KafkaTLSCAFile)
		if err != nil {
			return nil, err
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka TLS CA file does not contain any PEM certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if cfg.KafkaTLSCertFile != "" || cfg.KafkaTLSKeyFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.KafkaTLSCertFile, cfg.KafkaTLSKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}
