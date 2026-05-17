package runtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	kafkaeventbus "github.com/simpletrack/analytics-core/eventbus/kafka"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func TestNewSourceResolverBindsHTTPResolverToIngestionSchemaSurface(t *testing.T) {
	// Return a valid control-plane source that intentionally differs from the
	// boot-time source list. The write key still matches so only schema-surface
	// binding can reject it.
	startup := testRuntimeSource()
	remote := testRuntimeSource()
	remote.SourceID = "source_missing_startup_schema"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remote)
	}))
	defer server.Close()

	// Enable same-process ingestion with HTTP source resolution. This is the
	// runtime join point where dynamic control-plane config must be gated by the
	// startup schema surface before collect can publish.
	resolver, err := newSourceResolver(config.Config{
		SourceResolver:                    "http",
		ControlPlaneURL:                   server.URL,
		ControlPlaneToken:                 "runtime-token",
		ControlPlaneAllowInsecureLoopback: true,
		IngestionEnabled:                  true,
		Sources:                           []controlplane.SourceConfig{startup},
	})
	if err != nil {
		t.Fatalf("new source resolver failed: %v", err)
	}

	// Resolve through the assembled runtime resolver rather than the wrapper
	// directly, guarding against future refactors that forget the binding step.
	_, err = resolver.ResolveSource(context.Background(), startup.WriteKey)
	if !errors.Is(err, controlplane.ErrSourceOutsideSchemaSurface) {
		t.Fatalf("expected schema surface rejection, got %v", err)
	}
}

func TestNewKafkaTLSConfigUsesCAAndServerName(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, []byte(testCertificatePEM(t)), 0o600); err != nil {
		t.Fatalf("write ca file failed: %v", err)
	}

	tlsConfig, err := newKafkaTLSConfig(config.Config{
		KafkaTLSEnabled:            true,
		KafkaTLSServerName:         "kafka.example.com",
		KafkaTLSCAFile:             caFile,
		KafkaTLSInsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("new kafka TLS config failed: %v", err)
	}
	if tlsConfig == nil || tlsConfig.ServerName != "kafka.example.com" || tlsConfig.RootCAs == nil {
		t.Fatalf("unexpected TLS config: %#v", tlsConfig)
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected insecure skip verify test knob to be mapped")
	}
}

func TestNewKafkaTLSConfigRejectsInvalidCAFile(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "broken-ca.pem")
	if err := os.WriteFile(caFile, []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write ca file failed: %v", err)
	}

	_, err := newKafkaTLSConfig(config.Config{KafkaTLSEnabled: true, KafkaTLSCAFile: caFile})
	if err == nil {
		t.Fatal("expected invalid Kafka TLS CA file to fail")
	}
}

func TestKafkaEventBusStatsReturnsFalseWithoutKafka(t *testing.T) {
	runtime := &Runtime{}

	if stats, ok := runtime.KafkaEventBusStats(); ok || stats.Topic != "" {
		t.Fatalf("unexpected Kafka stats for non-Kafka runtime: ok=%v stats=%#v", ok, stats)
	}
}

func TestKafkaDiagnosticsResponseFromStatsMapsDiagnosticSnapshot(t *testing.T) {
	stats := kafkaeventbus.Stats{
		Topic:           "analytics.events",
		DeadLetterTopic: "analytics.events.dead",
		WorkerPool: kafkaeventbus.WorkerPoolStats{
			Name:            "kafka-eventbus-handler",
			GoroutinesTotal: 12,
			Queued:          10,
			QueueCapacity:   20,
			QueueUsageRatio: 0.5,
			Workers:         5,
			SubmittedTotal:  30,
			CompletedTotal:  19,
			RejectedTotal:   1,
			Closed:          true,
		},
		CompletionGate: kafkaeventbus.CompletionGateStats{
			InFlightMessages:  2,
			WaitingTasks:      4,
			CompletedMessages: 18,
		},
		Commits: []kafkaeventbus.OrderedCommitStats{
			{
				Topic:               "analytics.events",
				Partition:           2,
				Initialized:         true,
				NextOffset:          10,
				HighWaterMarkOffset: 17,
				Lag:                 7,
				PendingCount:        2,
				DoneCount:           1,
				OldestPendingOffset: 10,
				LargestPendingGap:   3,
			},
		},
		Paused: map[string][]int32{"analytics.events": {2}},
		Metrics: kafkaeventbus.MetricsStats{
			ConsumedTotal:          40,
			HandlerSuccessTotal:    35,
			HandlerFailureTotal:    4,
			HandlerRetryTotal:      3,
			MalformedTotal:         1,
			DeadLetterSuccessTotal: 2,
			DeadLetterFailureTotal: 1,
			PausedPartitions:       1,
			PauseTransitionsTotal:  2,
			ResumeTransitionsTotal: 1,
		},
	}

	response := kafkaDiagnosticsResponseFromStats(stats)

	if response.Topic != stats.Topic || response.DeadLetterTopic != stats.DeadLetterTopic {
		t.Fatalf("unexpected topics in diagnostics response: %#v", response)
	}
	if response.WorkerPool.QueueUsageRatio != stats.WorkerPool.QueueUsageRatio || !response.WorkerPool.Closed {
		t.Fatalf("unexpected worker pool mapping: %#v", response.WorkerPool)
	}
	if len(response.Commits) != 1 || response.Commits[0].LagEstimate != stats.Commits[0].Lag || response.Commits[0].NextOffset != stats.Commits[0].NextOffset {
		t.Fatalf("unexpected commit mapping: %#v", response.Commits)
	}
	if response.Metrics.DeadLetterFailureTotal != stats.Metrics.DeadLetterFailureTotal || response.Metrics.PausedPartitions != stats.Metrics.PausedPartitions {
		t.Fatalf("unexpected metrics mapping: %#v", response.Metrics)
	}
	stats.Paused["analytics.events"][0] = 9
	if response.Paused["analytics.events"][0] != 2 {
		t.Fatalf("expected paused partitions to be cloned, got %#v", response.Paused)
	}
	if empty := clonePausedPartitions(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("expected empty paused map for nil provider state, got %#v", empty)
	}
}

func TestNewKafkaMetricsSourceRequiresKafkaBus(t *testing.T) {
	_, err := newKafkaMetricsSource(config.Config{KafkaMetricsEnabled: true}, nil)
	if err == nil {
		t.Fatal("expected kafka metrics source without Kafka bus to fail")
	}

	source, err := newKafkaMetricsSource(config.Config{}, nil)
	if err != nil {
		t.Fatalf("expected disabled kafka metrics source to succeed: %v", err)
	}
	if source != nil {
		t.Fatalf("expected disabled kafka metrics source to be nil")
	}
}

func TestAllowedPropertySelectorsUseEnabledStartupSources(t *testing.T) {
	enabled := testRuntimeSource()
	enabled.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
		{Scope: "user", Name: "plan", ValueTypes: []string{"string"}},
	}
	disabled := testRuntimeSource()
	disabled.WriteKey = "wk_disabled"
	disabled.SourceID = "source_disabled"
	disabled.Enabled = false
	disabled.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "hidden", ValueTypes: []string{"string"}},
	}

	selectors := allowedPropertySelectors([]controlplane.SourceConfig{enabled, disabled})

	if len(selectors) != 2 {
		t.Fatalf("expected enabled source selectors only, got %#v", selectors)
	}
	assertHasSelector(t, selectors, storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"})
	assertHasSelector(t, selectors, storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "plan"})
}

func TestNewGeoResolverReturnsNilWhenUnconfigured(t *testing.T) {
	resolver, closer, err := newGeoResolver(config.Config{})
	if err != nil {
		t.Fatalf("expected empty geo config to succeed: %v", err)
	}
	if resolver != nil || closer != nil {
		t.Fatalf("expected nil geo resolver when file is unset, got %v %v", resolver, closer)
	}
}

func TestNewGeoResolverRejectsInvalidPath(t *testing.T) {
	_, _, err := newGeoResolver(config.Config{GeoIPMMDBFile: filepath.Join(t.TempDir(), "missing.mmdb")})
	if err == nil {
		t.Fatal("expected invalid geoip path to fail")
	}
}

func assertHasSelector(t *testing.T, selectors []storage.PropertySelector, want storage.PropertySelector) {
	t.Helper()

	for _, got := range selectors {
		if got == want {
			return
		}
	}
	t.Fatalf("expected selector %#v in %#v", want, selectors)
}

func testCertificatePEM(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate certificate key failed: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate failed: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
