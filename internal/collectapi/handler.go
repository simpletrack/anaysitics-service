package collectapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/netip"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/valyala/fasthttp"
)

const contentTypeJSON = "application/json"

// Options configures the analytics HTTP runtime.
type Options struct {
	CollectPath           string                // CollectPath is the POST route used by browser and server SDKs
	HealthPath            string                // HealthPath is the GET route used by process health checks
	TrackerPath           string                // TrackerPath is the GET route used to serve the browser tracker
	TrustForwardedHeaders bool                  // TrustForwardedHeaders enables proxy-provided client address headers
	TrackerScript         []byte                // TrackerScript is the JavaScript asset returned by TrackerPath
	Now                   collect.Clock         // Now supplies deterministic receive time for tests
	Resolver              controlplane.Resolver // Resolver reads runtime source configuration owned by the SaaS control plane
	Bus                   eventbus.EventBus     // Bus receives validated analytics events for ingestion
}

// Handler routes health, tracker, and collect requests.
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

// NewHandler creates an analytics runtime HTTP handler.
func NewHandler(opts Options) (*Handler, error) {
	if opts.CollectPath == "" {
		opts.CollectPath = "/collect"
	}
	if opts.HealthPath == "" {
		opts.HealthPath = "/healthz"
	}
	if opts.TrackerPath == "" {
		opts.TrackerPath = "/tracker.js"
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
	return &Handler{opts: opts}, nil
}

// ServeFastHTTP handles one fasthttp request.
func (h *Handler) ServeFastHTTP(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	switch {
	case ctx.IsGet() && path == h.opts.HealthPath:
		h.writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "ok"})
	case ctx.IsGet() && path == h.opts.TrackerPath:
		h.writeTracker(ctx)
	case ctx.IsOptions() && path == h.opts.CollectPath:
		h.writePreflight(ctx)
	case ctx.IsPost() && path == h.opts.CollectPath:
		h.handleCollect(ctx)
	default:
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
	}
}

func (h *Handler) handleCollect(ctx *fasthttp.RequestCtx) {
	// Decode the public request body first. Invalid JSON never reaches source
	// resolution or analytics-core validation because it is not a collect event.
	var payload collectPayload
	if err := json.Unmarshal(ctx.PostBody(), &payload); err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: "invalid collect payload"})
		return
	}

	// Resolve the runtime source before normalizing analytics identifiers. The
	// write key selects a SaaS-managed source config but does not let the client
	// choose tenant, project, source, or source type.
	writeKey := h.writeKey(ctx, payload.WriteKey)
	source, err := h.opts.Resolver.ResolveSource(ctx, writeKey)
	if err != nil {
		h.writeResolveError(ctx, err)
		return
	}

	// Enforce browser origin policy before building the analytics-core request.
	// A blocked origin is rejected and never enters queue, storage, or audit
	// paths as an accepted event.
	if !source.AllowsOrigin(requestOrigin(ctx)) {
		h.writeJSON(ctx, fasthttp.StatusForbidden, ErrorResponse{Error: "origin is not allowed"})
		return
	}
	h.writeCORS(ctx, requestOrigin(ctx))

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
		h.writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	// Build the analytics-core handler after runtime enforcement has sealed the
	// source scope. Construction errors are dependency bugs, not client input.
	handler, err := collect.NewHandlerWithOptions(h.opts.Bus, h.opts.Now, collect.WithStages(stages...))
	if err != nil {
		log.Printf("build collect handler: %v", err)
		h.writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
		return
	}

	// Publish through analytics-core. Validation errors are safe to expose;
	// queue/runtime errors are hidden behind a stable 5xx response.
	envelope, err := handler.Handle(ctx, request)
	if err != nil {
		h.writeCollectError(ctx, envelope, err)
		return
	}

	// Return acceptance only after EventBus.Publish succeeds. For the default
	// Redis runtime this means the event has reached the configured stream.
	h.writeJSON(ctx, fasthttp.StatusAccepted, AcceptedResponse{
		ID:         envelope.ID,
		ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
	})
}

func (h *Handler) stages(source controlplane.SourceConfig) ([]collect.Stage, error) {
	stages := make([]collect.Stage, 0, 3)

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
		HashSalt:         source.ClientHashSalt,
		IncludeUserAgent: true,
		IncludeIPHash:    true,
		IncludeReferrer:  true,
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
	return stages, nil
}

func (h *Handler) writeCollectError(ctx *fasthttp.RequestCtx, envelope contracts.EventEnvelope, err error) {
	var filteredErr collect.FilteredError
	if errors.As(err, &filteredErr) {
		if envelope.ID == "" {
			envelope = filteredErr.Envelope
		}
		h.writeJSON(ctx, fasthttp.StatusAccepted, AcceptedResponse{
			ID:         envelope.ID,
			ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
			Filtered:   true,
		})
		return
	}

	var validationErr collect.ValidationError
	if errors.As(err, &validationErr) {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: validationErr.Error()})
		return
	}
	log.Printf("collect publish failed: %v", err)
	h.writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
}

func (h *Handler) writeResolveError(ctx *fasthttp.RequestCtx, err error) {
	switch {
	case errors.Is(err, controlplane.ErrSourceNotFound):
		h.writeJSON(ctx, fasthttp.StatusUnauthorized, ErrorResponse{Error: "invalid write key"})
	case errors.Is(err, controlplane.ErrSourceDisabled):
		h.writeJSON(ctx, fasthttp.StatusForbidden, ErrorResponse{Error: "source is disabled"})
	default:
		log.Printf("resolve source failed: %v", err)
		h.writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}
}

func (h *Handler) writePreflight(ctx *fasthttp.RequestCtx) {
	// Preflight cannot prove write-key ownership because browsers do not send
	// the eventual request body. POST still performs the authoritative source
	// and origin checks before analytics-core sees the event.
	h.writeCORS(ctx, requestOrigin(ctx))
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	ctx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type, X-SimpleTrack-Write-Key, Authorization")
	ctx.Response.Header.Set("Access-Control-Max-Age", "600")
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (h *Handler) writeTracker(ctx *fasthttp.RequestCtx) {
	if len(h.opts.TrackerScript) == 0 {
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "tracker script is not configured"})
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/javascript; charset=utf-8")
	ctx.Response.Header.Set("Cache-Control", "public, max-age=300")
	ctx.SetBody(h.opts.TrackerScript)
}

func (h *Handler) writeJSON(ctx *fasthttp.RequestCtx, statusCode int, response any) {
	ctx.SetStatusCode(statusCode)
	ctx.Response.Header.SetContentType(contentTypeJSON)
	payload, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(`{"error":"failed to encode response"}`)
		return
	}
	ctx.SetBody(payload)
}

func (h *Handler) writeCORS(ctx *fasthttp.RequestCtx, origin string) {
	ctx.Response.Header.Set("Vary", "Origin")
	if origin != "" {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", origin)
	}
}

func (h *Handler) writeKey(ctx *fasthttp.RequestCtx, bodyValue string) string {
	if value := strings.TrimSpace(string(ctx.Request.Header.Peek("X-SimpleTrack-Write-Key"))); value != "" {
		return value
	}
	if value := bearerToken(string(ctx.Request.Header.Peek("Authorization"))); value != "" {
		return value
	}
	if value := strings.TrimSpace(string(ctx.QueryArgs().Peek("write_key"))); value != "" {
		return value
	}
	return strings.TrimSpace(bodyValue)
}

func (h *Handler) clientInfo(ctx *fasthttp.RequestCtx) collect.ClientInfo {
	return collect.ClientInfo{
		UserAgent: string(ctx.UserAgent()),
		IP:        h.clientIP(ctx),
		Referrer:  string(ctx.Request.Header.Peek("Referer")),
	}
}

func (h *Handler) clientIP(ctx *fasthttp.RequestCtx) string {
	if h.opts.TrustForwardedHeaders {
		if forwarded := firstHeaderValue(ctx.Request.Header.Peek("X-Forwarded-For")); forwarded != "" {
			if addr := canonicalClientIP(forwarded); addr != "" {
				return addr
			}
		}
		if realIP := strings.TrimSpace(string(ctx.Request.Header.Peek("X-Real-IP"))); realIP != "" {
			if addr := canonicalClientIP(realIP); addr != "" {
				return addr
			}
		}
	}
	if remoteIP := ctx.RemoteIP(); remoteIP != nil {
		return canonicalClientIP(remoteIP.String())
	}
	return ""
}

type collectPayload struct {
	collect.Request
	WriteKey string `json:"write_key"` // WriteKey is the runtime source secret, never trusted for tenant mapping
}

func requestOrigin(ctx *fasthttp.RequestCtx) string {
	return strings.TrimSpace(string(ctx.Request.Header.Peek("Origin")))
}

func bearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < len("Bearer ") || !strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(value[len("Bearer "):])
}

func firstHeaderValue(value []byte) string {
	text := strings.TrimSpace(string(value))
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

var _ fasthttp.RequestHandler = (&Handler{}).ServeFastHTTP
