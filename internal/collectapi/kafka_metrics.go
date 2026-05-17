package collectapi

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

const kafkaMetricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// KafkaMetricsSource returns a process-local Kafka provider snapshot for metrics export.
type KafkaMetricsSource func() (KafkaDiagnosticsResponse, bool)

// prometheusMetric describes one Prometheus text exposition sample.
type prometheusMetric struct {
	Name   string            // Name is the Prometheus sample name without labels
	Help   string            // Help describes the metric for Prometheus scrapers and operators
	Type   string            // Type is counter or gauge in Prometheus text exposition format
	Labels map[string]string // Labels are optional low-cardinality dimensions for one sample
	Value  float64           // Value is the numeric sample value
}

// metricNumber is the bounded numeric input surface accepted by metrics helpers.
type metricNumber interface {
	~float64 | ~int | ~int32 | ~int64
}

// handleKafkaMetrics writes a Prometheus text snapshot for Kafka EventBus state.
func (h *Handler) handleKafkaMetrics(ctx fiber.Ctx) error {
	// Metrics are process-scoped like diagnostics, so auth stops at route scope and
	// intentionally does not resolve write_key or inspect source readback policy.
	if h.opts.KafkaMetrics == nil {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	if _, ok := h.requireProcessQueryToken(ctx, controlplane.ReadbackRouteKafkaMetrics); !ok {
		return nil
	}

	// Snapshot after auth to avoid leaking whether Kafka is enabled to unauthenticated
	// probes. The text format exports counters and local pressure gauges only.
	stats, ok := h.opts.KafkaMetrics()
	if !ok {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	ctx.Set("Content-Type", kafkaMetricsContentType)
	return ctx.Status(fiber.StatusOK).SendString(renderKafkaMetrics(stats))
}

// renderKafkaMetrics converts one diagnostic snapshot into Prometheus text.
func renderKafkaMetrics(stats KafkaDiagnosticsResponse) string {
	metrics := kafkaPrometheusMetrics(stats)
	var builder strings.Builder
	emittedMetadata := map[string]struct{}{}
	for _, metric := range metrics {
		// Emit HELP and TYPE once per metric family before the first sample, matching
		// Prometheus text exposition parsers that reject duplicate metadata lines.
		if _, ok := emittedMetadata[metric.Name]; !ok {
			emittedMetadata[metric.Name] = struct{}{}
			builder.WriteString("# HELP ")
			builder.WriteString(metric.Name)
			builder.WriteByte(' ')
			builder.WriteString(metric.Help)
			builder.WriteByte('\n')
			builder.WriteString("# TYPE ")
			builder.WriteString(metric.Name)
			builder.WriteByte(' ')
			builder.WriteString(metric.Type)
			builder.WriteByte('\n')
		}
		builder.WriteString(metric.Name)
		writePrometheusLabels(&builder, metric.Labels)
		builder.WriteByte(' ')
		builder.WriteString(strconv.FormatFloat(metric.Value, 'f', -1, 64))
		builder.WriteByte('\n')
	}
	return builder.String()
}

// kafkaPrometheusMetrics selects the stable Kafka counters and pressure gauges.
func kafkaPrometheusMetrics(stats KafkaDiagnosticsResponse) []prometheusMetric {
	metrics := []prometheusMetric{
		counterMetric("simpletrack_kafka_consumed_total", "Kafka records consumed from the primary analytics topic.", stats.Metrics.ConsumedTotal),
		counterMetric("simpletrack_kafka_handler_success_total", "Kafka records completed through handler success.", stats.Metrics.HandlerSuccessTotal),
		counterMetric("simpletrack_kafka_handler_failure_total", "Kafka handler attempts that returned an error.", stats.Metrics.HandlerFailureTotal),
		counterMetric("simpletrack_kafka_handler_retry_total", "Kafka handler attempts scheduled after a previous failure.", stats.Metrics.HandlerRetryTotal),
		counterMetric("simpletrack_kafka_malformed_total", "Kafka records that could not decode as EventEnvelope.", stats.Metrics.MalformedTotal),
		counterMetric("simpletrack_kafka_dead_letter_success_total", "Kafka records successfully written to the dead-letter topic.", stats.Metrics.DeadLetterSuccessTotal),
		counterMetric("simpletrack_kafka_dead_letter_failure_total", "Kafka dead-letter write attempts that failed.", stats.Metrics.DeadLetterFailureTotal),
		counterMetric("simpletrack_kafka_pause_transitions_total", "Kafka local backpressure pause transitions.", stats.Metrics.PauseTransitionsTotal),
		counterMetric("simpletrack_kafka_resume_transitions_total", "Kafka local backpressure resume transitions.", stats.Metrics.ResumeTransitionsTotal),
		gaugeMetric("simpletrack_kafka_paused_partitions", "Kafka topic-partitions currently paused by local backpressure.", stats.Metrics.PausedPartitions),
		gaugeMetric("simpletrack_kafka_worker_queue_usage_ratio", "Kafka handler queue usage ratio in this process.", stats.WorkerPool.QueueUsageRatio),
		gaugeMetric("simpletrack_kafka_worker_queued", "Kafka handler tasks currently queued in this process.", stats.WorkerPool.Queued),
		gaugeMetric("simpletrack_kafka_worker_queue_capacity", "Kafka handler queue capacity in this process.", stats.WorkerPool.QueueCapacity),
		gaugeMetric("simpletrack_kafka_worker_workers", "Kafka handler worker count in this process.", stats.WorkerPool.Workers),
		counterMetric("simpletrack_kafka_worker_submitted_total", "Kafka handler tasks accepted by the worker pool.", stats.WorkerPool.SubmittedTotal),
		counterMetric("simpletrack_kafka_worker_completed_total", "Kafka handler tasks completed by the worker pool.", stats.WorkerPool.CompletedTotal),
		counterMetric("simpletrack_kafka_worker_rejected_total", "Kafka handler tasks rejected by the worker pool.", stats.WorkerPool.RejectedTotal),
		gaugeMetric("simpletrack_kafka_completion_in_flight_messages", "Kafka messages not yet completed by the completion gate.", stats.CompletionGate.InFlightMessages),
		gaugeMetric("simpletrack_kafka_completion_waiting_tasks", "Kafka async tasks still waiting in the completion gate.", stats.CompletionGate.WaitingTasks),
		counterMetric("simpletrack_kafka_completion_completed_messages_total", "Kafka messages completed by the completion gate.", stats.CompletionGate.CompletedMessages),
	}
	for _, commit := range stats.Commits {
		labels := map[string]string{
			"topic":     commit.Topic,
			"partition": strconv.FormatInt(int64(commit.Partition), 10),
		}
		metrics = append(metrics,
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_initialized", "Kafka ordered committer initialization state by topic-partition.", labels, boolFloat(commit.Initialized)),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_next_offset", "Kafka earliest offset still blocking ordered completion by topic-partition.", labels, commit.NextOffset),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_high_watermark_offset", "Kafka latest high-water mark observed by this process by topic-partition.", labels, commit.HighWaterMarkOffset),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_lag_estimate", "Kafka process-local lag estimate by topic-partition; not authoritative broker lag.", labels, commit.LagEstimate),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_pending", "Kafka registered offsets not yet completed by topic-partition.", labels, commit.PendingCount),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_done", "Kafka completed offsets waiting for earlier offsets by topic-partition.", labels, commit.DoneCount),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_oldest_pending_offset", "Kafka oldest registered pending offset by topic-partition.", labels, commit.OldestPendingOffset),
			labeledGaugeMetric("simpletrack_kafka_ordered_commit_largest_pending_gap", "Kafka largest observed registration gap by topic-partition.", labels, commit.LargestPendingGap),
		)
	}
	return metrics
}

// counterMetric builds one Prometheus counter sample.
func counterMetric[T metricNumber](name string, help string, value T) prometheusMetric {
	return prometheusMetric{Name: name, Help: help, Type: "counter", Value: float64(value)}
}

// gaugeMetric builds one Prometheus gauge sample.
func gaugeMetric[T metricNumber](name string, help string, value T) prometheusMetric {
	return prometheusMetric{Name: name, Help: help, Type: "gauge", Value: float64(value)}
}

// labeledGaugeMetric builds one labeled Prometheus gauge sample.
func labeledGaugeMetric[T metricNumber](name string, help string, labels map[string]string, value T) prometheusMetric {
	return prometheusMetric{Name: name, Help: help, Type: "gauge", Labels: labels, Value: float64(value)}
}

// boolFloat encodes a boolean gauge value as 1 or 0.
func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// writePrometheusLabels writes the low-cardinality labels used by commit metrics.
func writePrometheusLabels(builder *strings.Builder, labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	keys := []string{"topic", "partition"}
	builder.WriteByte('{')
	for idx, key := range keys {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteString("=\"")
		builder.WriteString(escapePrometheusLabel(labels[key]))
		builder.WriteByte('"')
	}
	builder.WriteByte('}')
}

// escapePrometheusLabel escapes label values for Prometheus text exposition.
func escapePrometheusLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, "\"", "\\\"")
}
