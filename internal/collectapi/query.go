package collectapi

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/valyala/fasthttp"
)

const (
	queryRealtimePath       = "/v1/realtime"
	queryEventsPath         = "/v1/events"
	defaultRealtimeWindow   = 30 * time.Minute
	defaultRealtimeQueryCap = 50
	defaultEventsQueryCap   = 100
	queryAllowHeaders       = "Content-Type, Authorization, X-SimpleTrack-Write-Key"
)

type querySourceResponse struct {
	TenantID   string `json:"tenant_id"`
	ProjectID  string `json:"project_id"`
	SourceID   string `json:"source_id"`
	SourceType string `json:"source_type"`
}

type queryEventResponse struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	ProjectID      string          `json:"project_id"`
	SourceID       string          `json:"source_id"`
	SourceType     string          `json:"source_type"`
	EventName      string          `json:"event_name"`
	DistinctID     string          `json:"distinct_id"`
	SessionID      string          `json:"session_id,omitempty"`
	EventTime      string          `json:"event_time"`
	ReceivedAt     string          `json:"received_at"`
	Properties     json.RawMessage `json:"properties,omitempty"`
	UserProperties json.RawMessage `json:"user_properties,omitempty"`
	Source         string          `json:"source,omitempty"`
}

type queryEventsResponse struct {
	Source querySourceResponse  `json:"source"`
	Items  []queryEventResponse `json:"items"`
	Limit  int                  `json:"limit"`
	Offset int                  `json:"offset"`
	From   string               `json:"from"`
	To     string               `json:"to"`
}

type queryRealtimeResponse struct {
	Source querySourceResponse  `json:"source"`
	Items  []queryEventResponse `json:"items"`
	Since  string               `json:"since"`
	Limit  int                  `json:"limit"`
}

func (h *Handler) handleRealtime(ctx *fasthttp.RequestCtx) {
	// Reject read routes when query support is not assembled. This keeps the
	// runtime shape explicit instead of surfacing a half-configured internal API.
	if h.opts.QueryReader == nil {
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
		return
	}
	h.writeQueryCORS(ctx)
	if !h.requireQueryToken(ctx) {
		return
	}

	// Resolve the runtime source first so the internal read API stays scoped to
	// the same write-key boundary as collect.
	source, ok := h.resolveQuerySource(ctx)
	if !ok {
		return
	}

	// Realtime uses a short recent window. Default to a 30 minute window when
	// the caller does not pin an explicit since timestamp.
	since, err := parseQueryTimeOrDefault(ctx, "since", h.opts.Now().Add(-defaultRealtimeWindow))
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	limit, err := parseQueryLimitOrDefault(ctx, "limit", defaultRealtimeQueryCap)
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Execute through analytics-core so the service only owns HTTP decoding and
	// response shaping, not query semantics.
	records, err := h.opts.QueryReader.ListRealtime(ctx, storage.RealtimeQuery{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
		Since:     since,
		Limit:     limit,
	})
	if err != nil {
		h.writeQueryError(ctx, err)
		return
	}

	h.writeJSON(ctx, fasthttp.StatusOK, queryRealtimeResponse{
		Source: toQuerySourceResponse(source),
		Items:  toQueryEventResponses(records),
		Since:  since.UTC().Format(time.RFC3339Nano),
		Limit:  limit,
	})
}

func (h *Handler) handleEvents(ctx *fasthttp.RequestCtx) {
	// Reject read routes when query support is not assembled. This keeps the
	// runtime shape explicit instead of surfacing a half-configured internal API.
	if h.opts.QueryReader == nil {
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
		return
	}
	h.writeQueryCORS(ctx)
	if !h.requireQueryToken(ctx) {
		return
	}

	// Resolve the runtime source first so readback cannot be pointed at a
	// different tenant/project/source than the write-key boundary.
	source, ok := h.resolveQuerySource(ctx)
	if !ok {
		return
	}

	// Events requires an explicit time range to keep the service from turning a
	// dashboard request into an open-ended historical scan.
	from, err := parseRequiredQueryTime(ctx, "from")
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	to, err := parseRequiredQueryTime(ctx, "to")
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	limit, err := parseQueryLimitOrDefault(ctx, "limit", defaultEventsQueryCap)
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	offset, err := parseQueryIntOrDefault(ctx, "offset", 0)
	if err != nil {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Let analytics-core own the allowlisted sort and filter semantics. The
	// service only maps the public query string into the core request contract.
	records, err := h.opts.QueryReader.ListEvents(ctx, storage.EventListQuery{
		TenantID:      source.TenantID,
		ProjectID:     source.ProjectID,
		SourceID:      source.SourceID,
		EventName:     strings.TrimSpace(string(ctx.QueryArgs().Peek("event_name"))),
		DistinctID:    strings.TrimSpace(string(ctx.QueryArgs().Peek("distinct_id"))),
		From:          from,
		To:            to,
		Limit:         limit,
		Offset:        offset,
		SortField:     storage.EventSortField(strings.TrimSpace(string(ctx.QueryArgs().Peek("sort_field")))),
		SortDirection: storage.EventSortDirection(strings.TrimSpace(string(ctx.QueryArgs().Peek("sort_direction")))),
	})
	if err != nil {
		h.writeQueryError(ctx, err)
		return
	}

	h.writeJSON(ctx, fasthttp.StatusOK, queryEventsResponse{
		Source: toQuerySourceResponse(source),
		Items:  toQueryEventResponses(records),
		Limit:  limit,
		Offset: offset,
		From:   from.UTC().Format(time.RFC3339Nano),
		To:     to.UTC().Format(time.RFC3339Nano),
	})
}

func (h *Handler) handleQueryPreflight(ctx *fasthttp.RequestCtx) {
	// Reject read routes when query support is not assembled. This keeps the
	// runtime shape explicit instead of surfacing a half-configured internal API.
	if h.opts.QueryReader == nil {
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
		return
	}

	// Query preflight mirrors collect preflight but uses GET instead of POST so
	// browser or SaaS-page callers can authorize readback before the actual
	// request carries a bearer token.
	h.writeQueryCORS(ctx)
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	ctx.Response.Header.Set("Access-Control-Allow-Headers", queryAllowHeaders)
	ctx.Response.Header.Set("Access-Control-Max-Age", "600")
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (h *Handler) requireQueryToken(ctx *fasthttp.RequestCtx) bool {
	// A missing accepted-token list means the internal read API was not safely
	// configured, so hide the route shape instead of returning auth details.
	if len(h.opts.QueryTokens) == 0 {
		h.writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
		return false
	}
	// Accept any configured rotation token while keeping source resolution and
	// query execution behind successful internal authentication.
	if !queryTokenAllowed(bearerToken(string(ctx.Request.Header.Peek("Authorization"))), h.opts.QueryTokens) {
		h.writeJSON(ctx, fasthttp.StatusUnauthorized, ErrorResponse{Error: "unauthorized"})
		return false
	}
	return true
}

func (h *Handler) resolveQuerySource(ctx *fasthttp.RequestCtx) (controlplane.SourceConfig, bool) {
	// Query routes stay on the same write-key boundary as collect, but they also
	// have to emit CORS headers for browser or SaaS-page callers before token
	// validation runs.
	writeKey := h.queryWriteKey(ctx)
	if writeKey == "" {
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: "write_key is required"})
		return controlplane.SourceConfig{}, false
	}
	source, err := h.opts.Resolver.ResolveSource(ctx, writeKey)
	if err != nil {
		h.writeResolveError(ctx, err)
		return controlplane.SourceConfig{}, false
	}
	origin := requestOrigin(ctx)
	if !source.AllowsOrigin(origin) {
		h.writeJSON(ctx, fasthttp.StatusForbidden, ErrorResponse{Error: "origin is not allowed"})
		return controlplane.SourceConfig{}, false
	}
	return source, true
}

func (h *Handler) writeQueryCORS(ctx *fasthttp.RequestCtx) {
	// Query routes are internal but may be called by the SaaS browser app. The
	// actual source allowlist check still runs after write-key resolution, while
	// early auth/validation failures remain readable by browser callers.
	h.writeCORS(ctx, requestOrigin(ctx))
}

func (h *Handler) queryWriteKey(ctx *fasthttp.RequestCtx) string {
	if value := strings.TrimSpace(string(ctx.Request.Header.Peek("X-SimpleTrack-Write-Key"))); value != "" {
		return value
	}
	return strings.TrimSpace(string(ctx.QueryArgs().Peek("write_key")))
}

func (h *Handler) writeQueryError(ctx *fasthttp.RequestCtx, err error) {
	switch {
	case errors.Is(err, storage.ErrInvalidEventQuery):
		h.writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: err.Error()})
	default:
		log.Printf("query failed: %v", err)
		h.writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}
}

func parseRequiredQueryTime(ctx *fasthttp.RequestCtx, key string) (time.Time, error) {
	value := strings.TrimSpace(string(ctx.QueryArgs().Peek(key)))
	if value == "" {
		return time.Time{}, errors.New(key + " is required")
	}
	return parseTimeValue(value)
}

func parseQueryTimeOrDefault(ctx *fasthttp.RequestCtx, key string, fallback time.Time) (time.Time, error) {
	value := strings.TrimSpace(string(ctx.QueryArgs().Peek(key)))
	if value == "" {
		return fallback.UTC(), nil
	}
	return parseTimeValue(value)
}

func parseQueryLimitOrDefault(ctx *fasthttp.RequestCtx, key string, fallback int) (int, error) {
	return normalizeQueryIntOrDefault(ctx, key, fallback, 1)
}

func parseQueryIntOrDefault(ctx *fasthttp.RequestCtx, key string, fallback int) (int, error) {
	return normalizeQueryIntOrDefault(ctx, key, fallback, 0)
}

func normalizeQueryIntOrDefault(ctx *fasthttp.RequestCtx, key string, fallback int, min int) (int, error) {
	value := strings.TrimSpace(string(ctx.QueryArgs().Peek(key)))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New(key + " must be an integer")
	}
	if parsed < min {
		return 0, errors.New(key + " must be greater than or equal to " + strconv.Itoa(min))
	}
	return parsed, nil
}

func parseTimeValue(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return parsed.UTC(), nil
	}
	parsed, err = time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, errors.New("time must use RFC3339 or RFC3339Nano")
	}
	return parsed.UTC(), nil
}

func toQuerySourceResponse(source controlplane.SourceConfig) querySourceResponse {
	return querySourceResponse{
		TenantID:   source.TenantID,
		ProjectID:  source.ProjectID,
		SourceID:   source.SourceID,
		SourceType: source.SourceType,
	}
}

func toQueryEventResponses(records []storage.EventRecord) []queryEventResponse {
	responses := make([]queryEventResponse, 0, len(records))
	for _, record := range records {
		responses = append(responses, toQueryEventResponse(record))
	}
	return responses
}

func toQueryEventResponse(record storage.EventRecord) queryEventResponse {
	return queryEventResponse{
		ID:             record.ID,
		TenantID:       record.TenantID,
		ProjectID:      record.ProjectID,
		SourceID:       record.SourceID,
		SourceType:     record.SourceType,
		EventName:      record.EventName,
		DistinctID:     record.DistinctID,
		SessionID:      record.SessionID,
		EventTime:      record.EventTime.UTC().Format(time.RFC3339Nano),
		ReceivedAt:     record.ReceivedAt.UTC().Format(time.RFC3339Nano),
		Properties:     queryJSON(record.Properties),
		UserProperties: queryJSON(record.UserProperties),
		Source:         record.Source,
	}
}

func queryJSON(value string) json.RawMessage {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	if json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	quoted, err := json.Marshal(trimmed)
	if err != nil {
		return nil
	}
	return quoted
}
