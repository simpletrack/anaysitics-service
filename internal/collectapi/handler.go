package collectapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/netip"
	"strings"
	"time"

	"github.com/gofiber/contrib/v3/swaggerui"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

const contentTypeJSON = "application/json"

// Options configures the analytics HTTP runtime.
type Options struct {
	CollectPath           string                        // CollectPath is the POST route used by browser and server SDKs
	HealthPath            string                        // HealthPath is the GET route used by process health checks
	TrackerPath           string                        // TrackerPath is the GET route used to serve the browser tracker
	EventsPath            string                        // EventsPath is the GET route used by internal Events readback
	GoalsPath             string                        // GoalsPath is the GET route used by internal Goal summary readback
	RealtimePath          string                        // RealtimePath is the GET route used by internal Realtime readback
	PropertiesPath        string                        // PropertiesPath is the GET route used by internal property catalog reads
	KafkaDiagnosticsPath  string                        // KafkaDiagnosticsPath is the GET route used by internal Kafka diagnostics
	KafkaMetricsPath      string                        // KafkaMetricsPath is the GET route used by internal Kafka metrics scraping
	SwaggerEnabled        bool                          // SwaggerEnabled exposes the generated OpenAPI documentation UI
	SwaggerPath           string                        // SwaggerPath is the Fiber route prefix for Swagger UI
	OpenAPIFile           string                        // OpenAPIFile is the local OpenAPI YAML or JSON file served by Swagger UI
	TrustForwardedHeaders bool                          // TrustForwardedHeaders enables proxy-provided client address headers
	TrackerScript         []byte                        // TrackerScript is the JavaScript asset returned by TrackerPath
	Now                   collect.Clock                 // Now supplies deterministic receive time for tests
	Resolver              controlplane.Resolver         // Resolver reads runtime source configuration owned by the SaaS control plane
	Bus                   eventbus.EventBus             // Bus receives validated analytics events for ingestion
	QueryReader           storage.EventReader           // QueryReader serves internal Events and Realtime readback when enabled
	PropertyCatalog       storage.PropertyCatalogReader // PropertyCatalog serves internal source-scoped property metadata reads
	QueryToken            string                        // QueryToken authorizes internal readback requests
	QueryTokens           []string                      // QueryTokens are accepted internal readback tokens during rotation windows
	QueryCredentials      []QueryCredential             // QueryCredentials are accepted internal readback tokens with lifecycle metadata
	KafkaDiagnostics      KafkaDiagnosticsSource        // KafkaDiagnostics returns process-local Kafka EventBus diagnostics when configured
	KafkaMetrics          KafkaMetricsSource            // KafkaMetrics returns process-local Kafka EventBus metrics when configured
	UserAgentParser       collect.UserAgentParser       // UserAgentParser optionally overrides analytics-core default UA parsing
	GeoResolver           collect.GeoResolver           // GeoResolver optionally resolves transient client IPs into coarse geography
}

// Handler routes health, tracker, collect, query, and documentation requests.
type Handler struct {
	opts Options // opts stores dependencies and route paths
}

// AcceptedResponse is returned when collect accepts a request.
type AcceptedResponse struct {
	ID         string `json:"id"`                 // ID is the accepted event id
	ReceivedAt string `json:"received_at"`        // ReceivedAt is the server acceptance timestamp
	Filtered   bool   `json:"filtered,omitempty"` // Filtered reports a valid event intentionally dropped before publish
}

// ErrorResponse is returned when the service rejects a request.
type ErrorResponse struct {
	Error string `json:"error"` // Error is the stable error summary
}

// NewApp creates a Fiber app for the analytics service HTTP boundary.
func NewApp(opts Options) (*fiber.App, error) {
	h, err := newHandler(opts)
	if err != nil {
		return nil, err
	}
	app := fiber.New(fiber.Config{
		ErrorHandler: func(ctx fiber.Ctx, err error) error {
			log.Printf("fiber request failed: %v", err)
			return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		},
	})
	app.Use(cors.New(cors.Config{
		AllowOriginsFunc: func(origin string) bool {
			return strings.TrimSpace(origin) != ""
		},
		AllowMethods: []string{fiber.MethodGet, fiber.MethodPost, fiber.MethodOptions},
		AllowHeaders: []string{"Content-Type", "Authorization", "X-SimpleTrack-Write-Key"},
		MaxAge:       600,
	}))
	h.registerRoutes(app)
	return app, nil
}

func newHandler(opts Options) (*Handler, error) {
	if opts.CollectPath == "" {
		opts.CollectPath = "/collect"
	}
	if opts.HealthPath == "" {
		opts.HealthPath = "/healthz"
	}
	if opts.TrackerPath == "" {
		opts.TrackerPath = "/tracker.js"
	}
	if opts.EventsPath == "" {
		opts.EventsPath = "/v1/events"
	}
	if opts.GoalsPath == "" {
		opts.GoalsPath = "/v1/goals"
	}
	if opts.RealtimePath == "" {
		opts.RealtimePath = "/v1/realtime"
	}
	if opts.PropertiesPath == "" {
		opts.PropertiesPath = "/v1/properties"
	}
	if opts.KafkaDiagnosticsPath == "" {
		opts.KafkaDiagnosticsPath = "/v1/kafka/diagnostics"
	}
	if opts.KafkaMetricsPath == "" {
		opts.KafkaMetricsPath = "/v1/kafka/metrics"
	}
	if opts.SwaggerPath == "" {
		opts.SwaggerPath = "/swagger"
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Resolver == nil {
		return nil, errors.New("control-plane resolver is required")
	}
	if opts.Bus == nil {
		return nil, errors.New("event bus is required")
	}
	// Collapse legacy single-token config and rotation allowlist config before
	// exposing the internal read routes.
	opts.QueryCredentials = normalizeQueryCredentials(opts.QueryToken, opts.QueryTokens, opts.QueryCredentials)
	opts.QueryTokens = nil
	if (opts.QueryReader != nil || opts.PropertyCatalog != nil || opts.KafkaDiagnostics != nil || opts.KafkaMetrics != nil) && len(opts.QueryCredentials) == 0 {
		return nil, errors.New("query token is required when internal read routes are configured")
	}
	if err := validateRoutePaths(opts); err != nil {
		return nil, err
	}
	return &Handler{opts: opts}, nil
}

func validateRoutePaths(opts Options) error {
	paths := map[string]string{
		"collect path":    opts.CollectPath,
		"health path":     opts.HealthPath,
		"tracker path":    opts.TrackerPath,
		"events path":     opts.EventsPath,
		"goals path":      opts.GoalsPath,
		"realtime path":   opts.RealtimePath,
		"properties path": opts.PropertiesPath,
	}
	if opts.KafkaDiagnostics != nil {
		paths["kafka diagnostics path"] = opts.KafkaDiagnosticsPath
	}
	if opts.KafkaMetrics != nil {
		paths["kafka metrics path"] = opts.KafkaMetricsPath
	}
	if opts.SwaggerEnabled {
		paths["swagger path"] = opts.SwaggerPath
	}
	seen := map[string]string{}
	for name, path := range paths {
		if strings.TrimSpace(path) == "" || !strings.HasPrefix(path, "/") {
			return errors.New(name + " must start with /")
		}
		if prior := seen[path]; prior != "" {
			return errors.New(name + " conflicts with " + prior)
		}
		seen[path] = name
	}
	if opts.SwaggerEnabled && strings.TrimSpace(opts.OpenAPIFile) == "" {
		return errors.New("openapi file is required when swagger is enabled")
	}
	return nil
}

func (h *Handler) registerRoutes(app *fiber.App) {
	app.Get(h.opts.HealthPath, h.handleHealth)
	app.Get(h.opts.TrackerPath, h.writeTracker)
	app.Post(h.opts.CollectPath, h.handleCollect)
	app.Get(h.opts.RealtimePath, h.handleRealtime)
	app.Get(h.opts.EventsPath, h.handleEvents)
	app.Get(h.opts.GoalsPath, h.handleGoals)
	app.Get(h.opts.PropertiesPath, h.handleProperties)
	if h.opts.KafkaDiagnostics != nil {
		app.Get(h.opts.KafkaDiagnosticsPath, h.handleKafkaDiagnostics)
	}
	if h.opts.KafkaMetrics != nil {
		app.Get(h.opts.KafkaMetricsPath, h.handleKafkaMetrics)
	}
	if h.opts.SwaggerEnabled {
		app.Use(h.opts.SwaggerPath, swaggerui.New(swaggerui.Config{
			BasePath: h.opts.SwaggerPath,
			FilePath: h.opts.OpenAPIFile,
			Title:    "SimpleTrack Analytics Service API",
		}))
	}
	app.Use(func(ctx fiber.Ctx) error {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	})
}

func (h *Handler) handleHealth(ctx fiber.Ctx) error {
	return h.writeJSON(ctx, fiber.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleCollect(ctx fiber.Ctx) error {
	// Decode the public request body first. Invalid JSON never reaches source
	// resolution or analytics-core validation because it is not a collect event.
	var payload collectPayload
	if err := json.Unmarshal(ctx.Body(), &payload); err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: "invalid collect payload"})
	}

	// Resolve the runtime source before normalizing analytics identifiers. The
	// write key selects a SaaS-managed source config but does not let the client
	// choose tenant, project, source, or source type.
	writeKey := h.writeKey(ctx, payload.WriteKey)
	source, err := h.opts.Resolver.ResolveSource(ctx.Context(), writeKey)
	if err != nil {
		return h.writeResolveError(ctx, err)
	}

	// Enforce browser origin policy before building the analytics-core request.
	// A blocked origin is rejected and never enters queue, storage, or audit
	// paths as an accepted event.
	if !source.AllowsOrigin(requestOrigin(ctx)) {
		return h.writeJSON(ctx, fiber.StatusForbidden, ErrorResponse{Error: "origin is not allowed"})
	}

	// Override all client-supplied scope fields with the control-plane runtime
	// config. This is the key trust boundary between public SDK payloads and
	// tenant/project/source isolation.
	request := payload.Request
	request.TenantID = source.TenantID
	request.ProjectID = source.ProjectID
	request.SourceID = source.SourceID
	request.SourceType = source.SourceType
	request.Client = h.clientInfo(ctx)

	// Build collect stages from runtime source config. Invalid source-side
	// filter or salt config is a server misconfiguration, so the public response
	// stays stable and the concrete detail is only logged.
	stages, err := h.stages(source)
	if err != nil {
		log.Printf("build collect stages: %v", err)
		return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}

	// Build the analytics-core handler after runtime enforcement has sealed the
	// source scope. Construction errors are dependency bugs, not client input.
	handler, err := collect.NewHandlerWithOptions(h.opts.Bus, h.opts.Now, collect.WithStages(stages...))
	if err != nil {
		log.Printf("build collect handler: %v", err)
		return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}

	// Publish through analytics-core. Validation errors are safe to expose;
	// queue/runtime errors are hidden behind a stable 5xx response.
	envelope, err := handler.Handle(ctx.Context(), request)
	if err != nil {
		return h.writeCollectError(ctx, envelope, err)
	}

	// Return acceptance only after EventBus.Publish succeeds. For durable
	// runtime providers this means the event has reached the configured queue.
	return h.writeJSON(ctx, fiber.StatusAccepted, AcceptedResponse{
		ID:         envelope.ID,
		ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
	})
}

func (h *Handler) stages(source controlplane.SourceConfig) ([]collect.Stage, error) {
	stages := make([]collect.Stage, 0, 4)

	// Runtime filters execute before queue publish so SaaS-managed internal
	// traffic rules do not write noise into analytics-core storage.
	filter, err := collect.NewTrafficFilterStage(collect.TrafficFilterConfig{
		BotUserAgents: source.BotUserAgents,
		InternalCIDRs: source.InternalCIDRs,
		InternalIPs:   source.InternalIPs,
	})
	if err == nil {
		stages = append(stages, filter)
	} else {
		return nil, err
	}

	// Enrichment uses transient client metadata but persists only bounded
	// derived properties. Raw IP addresses stay outside the event envelope.
	enrichment, err := collect.NewClientEnrichmentStage(collect.ClientEnrichmentConfig{
		HashSalt:           source.ClientHashSalt,
		IncludeUserAgent:   true,
		IncludeIPHash:      true,
		IncludeReferrer:    true,
		IncludeBrowserInfo: true,
		IncludeGeoInfo:     h.opts.GeoResolver != nil,
		UserAgentParser:    h.opts.UserAgentParser,
		GeoResolver:        h.opts.GeoResolver,
	})
	if err == nil {
		stages = append(stages, enrichment)
	} else {
		return nil, err
	}

	// Session derivation is configured by the control-plane runtime source and
	// remains deterministic for retries of the same event time bucket.
	session, err := collect.NewSessionResolverStage(collect.SessionResolverConfig{
		Salt:                     source.SessionSalt,
		IncludeClientFingerprint: source.IncludeClientFingerprint,
	})
	if err == nil {
		stages = append(stages, session)
	} else {
		return nil, err
	}

	// Visit derivation happens after session derivation because the canonical
	// analytics visit key should include the final stored session id when one is
	// present. The stage still preserves any SDK-provided visit_id.
	visit, err := collect.NewVisitResolverStage(collect.VisitResolverConfig{
		Salt:   source.VisitSalt,
		Window: source.VisitWindow,
	})
	if err == nil {
		stages = append(stages, visit)
	} else {
		return nil, err
	}
	return stages, nil
}

func (h *Handler) writeCollectError(ctx fiber.Ctx, envelope contracts.EventEnvelope, err error) error {
	var filteredErr collect.FilteredError
	if errors.As(err, &filteredErr) {
		if envelope.ID == "" {
			envelope = filteredErr.Envelope
		}
		log.Printf(
			"collect filtered: event_id=%s tenant_id=%s project_id=%s source_id=%s reason=%s",
			envelope.ID,
			envelope.TenantID,
			envelope.ProjectID,
			envelope.SourceID,
			filteredErr.Reason,
		)
		return h.writeJSON(ctx, fiber.StatusAccepted, AcceptedResponse{
			ID:         envelope.ID,
			ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
			Filtered:   true,
		})
	}

	var validationErr collect.ValidationError
	if errors.As(err, &validationErr) {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: validationErr.Error()})
	}
	log.Printf("collect publish failed: %v", err)
	return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
}

func (h *Handler) writeResolveError(ctx fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, controlplane.ErrSourceNotFound):
		return h.writeJSON(ctx, fiber.StatusUnauthorized, ErrorResponse{Error: "invalid write key"})
	case errors.Is(err, controlplane.ErrSourceDisabled):
		return h.writeJSON(ctx, fiber.StatusForbidden, ErrorResponse{Error: "source is disabled"})
	default:
		log.Printf("resolve source failed: %v", err)
		return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}
}

func (h *Handler) writeTracker(ctx fiber.Ctx) error {
	if len(h.opts.TrackerScript) == 0 {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "tracker script is not configured"})
	}
	ctx.Set("Content-Type", "application/javascript; charset=utf-8")
	ctx.Set("Cache-Control", "public, max-age=300")
	return ctx.Status(fiber.StatusOK).Send(h.opts.TrackerScript)
}

func (h *Handler) writeJSON(ctx fiber.Ctx, statusCode int, response any) error {
	return ctx.Status(statusCode).JSON(response, contentTypeJSON)
}

func (h *Handler) writeKey(ctx fiber.Ctx, bodyValue string) string {
	// Prefer explicit transport credentials over body fallback. The accepted
	// sources, in priority order, are:
	//  1. X-SimpleTrack-Write-Key: wk_header
	//  2. Authorization: Bearer wk_bearer
	//  3. ?write_key=wk_query
	//  4. JSON body write_key, passed here as bodyValue
	//
	// This lets browser SDKs, server SDKs, and quick manual tests use different
	// carriers while keeping one resolver boundary for tenant/project/source.
	if value := strings.TrimSpace(ctx.Get("X-SimpleTrack-Write-Key")); value != "" {
		return value
	}
	if value := bearerToken(ctx.Get("Authorization")); value != "" {
		return value
	}
	if value := strings.TrimSpace(ctx.Query("write_key")); value != "" {
		return value
	}
	return strings.TrimSpace(bodyValue)
}

func (h *Handler) clientInfo(ctx fiber.Ctx) collect.ClientInfo {
	return collect.ClientInfo{
		UserAgent: ctx.Get("User-Agent"),
		IP:        h.clientIP(ctx),
		Referrer:  ctx.Get("Referer"),
	}
}

func (h *Handler) clientIP(ctx fiber.Ctx) string {
	if h.opts.TrustForwardedHeaders {
		if forwarded := firstHeaderValue(ctx.Get("X-Forwarded-For")); forwarded != "" {
			if addr := canonicalClientIP(forwarded); addr != "" {
				return addr
			}
		}
		if realIP := strings.TrimSpace(ctx.Get("X-Real-IP")); realIP != "" {
			if addr := canonicalClientIP(realIP); addr != "" {
				return addr
			}
		}
	}
	return canonicalClientIP(ctx.IP())
}

type collectPayload struct {
	collect.Request
	WriteKey string `json:"write_key"` // WriteKey is the runtime source secret, never trusted for tenant mapping
}

func requestOrigin(ctx fiber.Ctx) string {
	return strings.TrimSpace(ctx.Get("Origin"))
}

func bearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < len("Bearer ") || !strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(value[len("Bearer "):])
}

func firstHeaderValue(value string) string {
	text := strings.TrimSpace(value)
	if comma := strings.IndexByte(text, ','); comma >= 0 {
		return strings.TrimSpace(text[:comma])
	}
	return text
}

func canonicalClientIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr.String()
	}
	addrPort, err := netip.ParseAddrPort(value)
	if err != nil {
		return ""
	}
	return addrPort.Addr().String()
}
