package collectapi

import (
	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

// KafkaDiagnosticsSource returns a process-local Kafka provider snapshot.
type KafkaDiagnosticsSource func() (KafkaDiagnosticsResponse, bool)

// KafkaDiagnosticsResponse describes the internal Kafka EventBus diagnostic payload.
//
// NOTE: The response is a point-in-time process snapshot for operators. It is
// not a broker-side lag source, billing record, audit log, or SLA metric.
type KafkaDiagnosticsResponse struct {
	Topic           string                          `json:"topic"`             // Topic is the primary analytics event topic
	DeadLetterTopic string                          `json:"dead_letter_topic"` // DeadLetterTopic is the configured DLQ topic
	WorkerPool      KafkaWorkerPoolDiagnostics      `json:"worker_pool"`       // WorkerPool reports bounded handler execution pressure
	CompletionGate  KafkaCompletionGateDiagnostics  `json:"completion_gate"`   // CompletionGate reports in-flight message and task pressure
	Commits         []KafkaOrderedCommitDiagnostics `json:"commits"`           // Commits reports per topic-partition ordered commit state
	Paused          map[string][]int32              `json:"paused"`            // Paused lists partitions currently paused by local backpressure
	Metrics         KafkaMetricsDiagnostics         `json:"metrics"`           // Metrics reports delivery, retry, and DLQ counters
}

// KafkaWorkerPoolDiagnostics reports Kafka handler pool pressure.
type KafkaWorkerPoolDiagnostics struct {
	Name            string  `json:"name"`              // Name identifies this pool in diagnostics
	GoroutinesTotal int     `json:"goroutines_total"`  // GoroutinesTotal is the runtime goroutine count at sampling time
	Queued          int64   `json:"queued"`            // Queued is the current number of queued handler tasks
	QueueCapacity   int     `json:"queue_capacity"`    // QueueCapacity is the bounded work queue capacity
	QueueUsageRatio float64 `json:"queue_usage_ratio"` // QueueUsageRatio is Queued divided by QueueCapacity
	Workers         int     `json:"workers"`           // Workers is the fixed handler worker count
	SubmittedTotal  int64   `json:"submitted_total"`   // SubmittedTotal is the lifetime accepted task count
	CompletedTotal  int64   `json:"completed_total"`   // CompletedTotal is the lifetime completed task count
	RejectedTotal   int64   `json:"rejected_total"`    // RejectedTotal is the lifetime rejected task count
	Closed          bool    `json:"closed"`            // Closed reports whether shutdown has started
}

// KafkaCompletionGateDiagnostics reports message completion gate pressure.
type KafkaCompletionGateDiagnostics struct {
	InFlightMessages  int64 `json:"in_flight_messages"` // InFlightMessages is the number of messages not yet completed
	WaitingTasks      int64 `json:"waiting_tasks"`      // WaitingTasks is the number of unfinished async tasks
	CompletedMessages int64 `json:"completed_messages"` // CompletedMessages is the lifetime count of completed messages
}

// KafkaOrderedCommitDiagnostics reports one topic-partition's ordered commit state.
type KafkaOrderedCommitDiagnostics struct {
	Topic               string `json:"topic"`                  // Topic is the Kafka topic name
	Partition           int32  `json:"partition"`              // Partition is the Kafka partition id
	Initialized         bool   `json:"initialized"`            // Initialized reports whether this partition has seen any offset
	NextOffset          int64  `json:"next_offset"`            // NextOffset is the earliest offset still blocking ordered completion
	HighWaterMarkOffset int64  `json:"high_water_mark_offset"` // HighWaterMarkOffset is the latest claim high-water mark observed from Kafka
	LagEstimate         int64  `json:"lag_estimate"`           // LagEstimate is a process-local estimate, not authoritative broker lag
	PendingCount        int    `json:"pending_count"`          // PendingCount is the number of registered unmarked offsets
	DoneCount           int    `json:"done_count"`             // DoneCount is the number of completed offsets waiting for earlier offsets
	OldestPendingOffset int64  `json:"oldest_pending_offset"`  // OldestPendingOffset is the earliest registered offset still pending
	LargestPendingGap   int64  `json:"largest_pending_gap"`    // LargestPendingGap is the largest observed registration gap
}

// KafkaMetricsDiagnostics reports provider-owned Kafka delivery counters.
type KafkaMetricsDiagnostics struct {
	ConsumedTotal          int64 `json:"consumed_total"`            // ConsumedTotal counts primary topic records pulled from Kafka
	HandlerSuccessTotal    int64 `json:"handler_success_total"`     // HandlerSuccessTotal counts records completed through handler success
	HandlerFailureTotal    int64 `json:"handler_failure_total"`     // HandlerFailureTotal counts failed handler attempts
	HandlerRetryTotal      int64 `json:"handler_retry_total"`       // HandlerRetryTotal counts handler attempts scheduled after a previous failure
	MalformedTotal         int64 `json:"malformed_total"`           // MalformedTotal counts records that could not decode as EventEnvelope
	DeadLetterSuccessTotal int64 `json:"dead_letter_success_total"` // DeadLetterSuccessTotal counts successful DLQ writes
	DeadLetterFailureTotal int64 `json:"dead_letter_failure_total"` // DeadLetterFailureTotal counts failed DLQ write attempts
	PausedPartitions       int64 `json:"paused_partitions"`         // PausedPartitions counts currently paused topic-partitions
	PauseTransitionsTotal  int64 `json:"pause_transitions_total"`   // PauseTransitionsTotal counts protector pause transitions
	ResumeTransitionsTotal int64 `json:"resume_transitions_total"`  // ResumeTransitionsTotal counts protector resume transitions
}

func (h *Handler) handleKafkaDiagnostics(ctx fiber.Ctx) error {
	// Keep diagnostics behind the same internal bearer-token lifecycle as query
	// readback, but do not require a write key because this snapshot is process
	// scoped rather than source scoped.
	if h.opts.KafkaDiagnostics == nil {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	if _, ok := h.requireProcessQueryToken(ctx, controlplane.ReadbackRouteKafkaDiagnostics); !ok {
		return nil
	}

	// Read the current provider snapshot only after auth succeeds so unauthenticated
	// probes cannot learn whether Kafka is enabled or which topics are configured.
	stats, ok := h.opts.KafkaDiagnostics()
	if !ok {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	return h.writeJSON(ctx, fiber.StatusOK, stats)
}
