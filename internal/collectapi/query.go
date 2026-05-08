package collectapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

const (
	defaultRealtimeWindow       = 30 * time.Minute
	defaultRealtimeQueryCap     = 50
	defaultEventsQueryCap       = 100
	defaultPropertyFilterCap    = 5
	defaultPropertyCatalogLimit = 100
	maxPropertyCatalogLimit     = 200
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
	VisitID        string          `json:"visit_id,omitempty"` // VisitID is the canonical analytics visit key returned to readback callers
	EventTime      string          `json:"event_time"`
	ReceivedAt     string          `json:"received_at"`
	Properties     json.RawMessage `json:"properties,omitempty"`
	UserProperties json.RawMessage `json:"user_properties,omitempty"`
	Source         string          `json:"source,omitempty"`
}

type queryEventsResponse struct {
	Source        querySourceResponse    `json:"source"`                   // Source reports the trusted runtime source boundary
	Items         []queryEventResponse   `json:"items"`                    // Items are the returned Events rows
	Limit         int                    `json:"limit"`                    // Limit is the effective caller-requested page size before core caps
	Offset        int                    `json:"offset"`                   // Offset is the Events pagination offset
	From          string                 `json:"from"`                     // From is the inclusive event-time lower bound
	To            string                 `json:"to"`                       // To is the exclusive event-time upper bound
	QueryEvidence *queryEvidenceResponse `json:"query_evidence,omitempty"` // QueryEvidence explains the read-side path chosen by analytics-core
}

type queryRealtimeResponse struct {
	Source        querySourceResponse    `json:"source"`                   // Source reports the trusted runtime source boundary
	Items         []queryEventResponse   `json:"items"`                    // Items are the returned Realtime rows
	Since         string                 `json:"since"`                    // Since is the inclusive recent-event lower bound
	Limit         int                    `json:"limit"`                    // Limit is the effective caller-requested page size before core caps
	QueryEvidence *queryEvidenceResponse `json:"query_evidence,omitempty"` // QueryEvidence explains the read-side path chosen by analytics-core
}

type propertyCatalogResponse struct {
	Source querySourceResponse           `json:"source"` // Source reports the trusted runtime source boundary
	Items  []propertyCatalogItemResponse `json:"items"`  // Items are observed source-scoped property definitions
	Limit  int                           `json:"limit"`  // Limit is the effective property catalog row cap
}

type propertyCatalogItemResponse struct {
	Scope       string `json:"scope"`         // Scope is event or user
	Name        string `json:"name"`          // Name is the normalized property key
	ValueType   string `json:"value_type"`    // ValueType is null, string, number, or bool
	FirstSeenAt string `json:"first_seen_at"` // FirstSeenAt is the earliest observed event timestamp
	LastSeenAt  string `json:"last_seen_at"`  // LastSeenAt is the latest observed event timestamp
}

type queryEvidenceResponse struct {
	Family              string `json:"family"`                // Family is the product query family, such as events or realtime
	ReadPath            string `json:"read_path"`             // ReadPath is the logical read model used by the query
	Optimization        string `json:"optimization"`          // Optimization is the physical acceleration strategy currently selected
	EffectiveLimit      int    `json:"effective_limit"`       // EffectiveLimit is the builder-capped row limit used by analytics-core
	Offset              int    `json:"offset"`                // Offset is the Events pagination offset after validation
	HasTimeLowerBound   bool   `json:"has_time_lower_bound"`  // HasTimeLowerBound reports whether the query constrains the start time
	HasTimeUpperBound   bool   `json:"has_time_upper_bound"`  // HasTimeUpperBound reports whether the query constrains the end time
	TimeWindowSeconds   int64  `json:"time_window_seconds"`   // TimeWindowSeconds is the bounded from/to window when both edges are present
	ScalarFilterCount   int    `json:"scalar_filter_count"`   // ScalarFilterCount counts non-property predicates
	PropertyFilterCount int    `json:"property_filter_count"` // PropertyFilterCount counts typed property predicates
	UsesPropertyTable   bool   `json:"uses_property_table"`   // UsesPropertyTable reports whether the typed property table participates
	SortField           string `json:"sort_field,omitempty"`  // SortField is the effective allowlisted sort field
	SortDirection       string `json:"sort_direction"`        // SortDirection is the effective allowlisted sort direction
	Pressure            string `json:"pressure"`              // Pressure is the coarse low/medium/high read-side triage bucket
}

const (
	pressureLow    = "low"
	pressureMedium = "medium"
	pressureHigh   = "high"
)

type propertyFilterPayload struct {
	Scope    string `json:"scope"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Operator string `json:"op"`
	Value    any    `json:"value"`
}

func (h *Handler) handleRealtime(ctx fiber.Ctx) error {
	// Reject read routes when query support is not assembled. This keeps the
	// runtime shape explicit instead of surfacing a half-configured internal API.
	if h.opts.QueryReader == nil {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	decision, ok := h.requireQueryToken(ctx)
	if !ok {
		return nil
	}

	// Resolve the runtime source first so the internal read API stays scoped to
	// the same write-key boundary as collect.
	source, ok := h.resolveQuerySource(ctx, decision)
	if !ok {
		return nil
	}

	// Realtime uses a short recent window. Default to a 30 minute window when
	// the caller does not pin an explicit since timestamp.
	since, err := parseQueryTimeOrDefault(ctx, "since", h.opts.Now().Add(-defaultRealtimeWindow))
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	limit, err := parseQueryLimitOrDefault(ctx, "limit", defaultRealtimeQueryCap)
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}

	// Execute through analytics-core so the service only owns HTTP decoding and
	// response shaping, not query semantics.
	records, evidence, err := h.listRealtime(ctx.Context(), storage.RealtimeQuery{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
		Since:     since,
		Limit:     limit,
	})
	if err != nil {
		return h.writeQueryError(ctx, err)
	}

	return h.writeJSON(ctx, fiber.StatusOK, queryRealtimeResponse{
		Source:        toQuerySourceResponse(source),
		Items:         toQueryEventResponses(records),
		Since:         since.UTC().Format(time.RFC3339Nano),
		Limit:         limit,
		QueryEvidence: evidence,
	})
}

func (h *Handler) handleEvents(ctx fiber.Ctx) error {
	// Reject read routes when query support is not assembled. This keeps the
	// runtime shape explicit instead of surfacing a half-configured internal API.
	if h.opts.QueryReader == nil {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	decision, ok := h.requireQueryToken(ctx)
	if !ok {
		return nil
	}

	// Resolve the runtime source first so readback cannot be pointed at a
	// different tenant/project/source than the write-key boundary.
	source, ok := h.resolveQuerySource(ctx, decision)
	if !ok {
		return nil
	}

	// Events requires an explicit time range to keep the service from turning a
	// dashboard request into an open-ended historical scan.
	from, err := parseRequiredQueryTime(ctx, "from")
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	to, err := parseRequiredQueryTime(ctx, "to")
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	limit, err := parseQueryLimitOrDefault(ctx, "limit", defaultEventsQueryCap)
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	offset, err := parseQueryIntOrDefault(ctx, "offset", 0)
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	propertyFilters, err := parsePropertyFilters(ctx, source)
	if err != nil {
		return h.writeQueryError(ctx, err)
	}

	// Let analytics-core own the allowlisted sort and filter semantics. The
	// service only maps the public query string into the core request contract.
	eventFilters := eventColumnFilters(ctx)
	records, evidence, err := h.listEvents(ctx.Context(), storage.EventListQuery{
		TenantID:                 source.TenantID,
		ProjectID:                source.ProjectID,
		SourceID:                 source.SourceID,
		EventName:                strings.TrimSpace(ctx.Query("event_name")),
		DistinctID:               strings.TrimSpace(ctx.Query("distinct_id")),
		Filters:                  eventFilters,
		From:                     from,
		To:                       to,
		Limit:                    limit,
		Offset:                   offset,
		SortField:                storage.EventSortField(strings.TrimSpace(ctx.Query("sort_field"))),
		SortDirection:            storage.EventSortDirection(strings.TrimSpace(ctx.Query("sort_direction"))),
		PropertyFilters:          propertyFilters,
		AllowedPropertySelectors: toPropertySelectors(source.AllowedPropertyFilters),
	})
	if err != nil {
		return h.writeQueryError(ctx, err)
	}

	return h.writeJSON(ctx, fiber.StatusOK, queryEventsResponse{
		Source:        toQuerySourceResponse(source),
		Items:         toQueryEventResponses(records),
		Limit:         limit,
		Offset:        offset,
		From:          from.UTC().Format(time.RFC3339Nano),
		To:            to.UTC().Format(time.RFC3339Nano),
		QueryEvidence: evidence,
	})
}

func (h *Handler) handleProperties(ctx fiber.Ctx) error {
	// The property catalog route is read-only metadata. It is available only
	// when the runtime assembled a catalog reader, usually from MySQL.
	if h.opts.PropertyCatalog == nil {
		return h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	}
	decision, ok := h.requireQueryToken(ctx)
	if !ok {
		return nil
	}

	// Reuse the same write-key and origin boundary as Events/Realtime so callers
	// cannot enumerate property selectors outside their source.
	source, ok := h.resolveQuerySource(ctx, decision)
	if !ok {
		return nil
	}

	scope, err := parsePropertyCatalogScope(ctx)
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}
	limit, err := parsePropertyCatalogLimit(ctx)
	if err != nil {
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	}

	// Delegate source-scoped metadata reads to analytics-core storage contracts.
	// The HTTP layer only maps query parameters and response shape.
	entries, err := h.opts.PropertyCatalog.ListPropertyCatalogEntries(ctx.Context(), storage.PropertyCatalogQuery{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
		Scope:     scope,
		Limit:     limit,
	})
	if err != nil {
		log.Printf("property catalog query failed: %v", err)
		return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}

	return h.writeJSON(ctx, fiber.StatusOK, propertyCatalogResponse{
		Source: toQuerySourceResponse(source),
		Items:  toPropertyCatalogItemResponses(entries),
		Limit:  limit,
	})
}

func (h *Handler) listRealtime(ctx context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, *queryEvidenceResponse, error) {
	// Prefer evidence-aware readers when available. The fallback keeps tests and
	// custom readers source-compatible while production ClickHouse readback can
	// surface read-side decisions to operators and SaaS pages.
	if reader, ok := h.opts.QueryReader.(storage.EventReaderWithEvidence); ok {
		result, err := reader.ListRealtimeWithEvidence(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		return result.Records, toQueryEvidenceResponse(result.Evidence), nil
	}
	records, err := h.opts.QueryReader.ListRealtime(ctx, query)
	return records, nil, err
}

func (h *Handler) listEvents(ctx context.Context, query storage.EventListQuery) ([]storage.EventRecord, *queryEvidenceResponse, error) {
	// Keep query evidence optional at the interface boundary. This avoids
	// coupling the HTTP layer to ClickHouse while still letting the standard
	// runtime expose the plan metadata produced by analytics-core.
	if reader, ok := h.opts.QueryReader.(storage.EventReaderWithEvidence); ok {
		result, err := reader.ListEventsWithEvidence(ctx, query)
		if err != nil {
			return nil, nil, err
		}
		return result.Records, toQueryEvidenceResponse(result.Evidence), nil
	}
	records, err := h.opts.QueryReader.ListEvents(ctx, query)
	return records, nil, err
}

func eventColumnFilters(ctx fiber.Ctx) []storage.EventFilter {
	filters := make([]storage.EventFilter, 0, 1)

	// visit_id is a persisted analytics key now, so expose it through the same
	// allowlisted filter path as future session/source fields instead of adding
	// ad hoc SQL or a service-owned query branch.
	if visitID := strings.TrimSpace(ctx.Query("visit_id")); visitID != "" {
		filters = append(filters, storage.EventFilter{
			Field:    storage.EventFilterByVisitID,
			Operator: storage.EventFilterEquals,
			Value:    visitID,
		})
	}
	return filters
}

func (h *Handler) requireQueryToken(ctx fiber.Ctx) (queryTokenAuthDecision, bool) {
	// A missing accepted-token list means the internal read API was not safely
	// configured, so hide the route shape instead of returning auth details.
	if len(h.opts.QueryCredentials) == 0 {
		_ = h.writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
		return queryTokenAuthDecision{State: queryTokenAuthUnknown}, false
	}
	// Accept any configured rotation token while keeping source resolution and
	// query execution behind successful internal authentication.
	decision := authorizeQueryToken(bearerToken(ctx.Get("Authorization")), h.opts.QueryCredentials, h.opts.Now())
	if decision.State == queryTokenAuthUnknown || decision.State == queryTokenAuthExpired || decision.State == queryTokenAuthNotYetValid {
		h.auditRejectedQueryToken(ctx, decision)
		_ = h.writeJSON(ctx, fiber.StatusUnauthorized, ErrorResponse{Error: "unauthorized"})
		return queryTokenAuthDecision{}, false
	}
	return decision, true
}

func (h *Handler) resolveQuerySource(ctx fiber.Ctx, decision queryTokenAuthDecision) (controlplane.SourceConfig, bool) {
	// Query routes stay on the same write-key boundary as collect, but they also
	// have to emit CORS headers for browser or SaaS-page callers before token
	// validation runs.
	writeKey := h.queryWriteKey(ctx)
	if writeKey == "" {
		_ = h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: "write_key is required"})
		return controlplane.SourceConfig{}, false
	}
	source, err := h.opts.Resolver.ResolveSource(ctx.Context(), writeKey)
	if err != nil {
		_ = h.writeResolveError(ctx, err)
		return controlplane.SourceConfig{}, false
	}
	origin := requestOrigin(ctx)
	if !source.AllowsOrigin(origin) {
		_ = h.writeJSON(ctx, fiber.StatusForbidden, ErrorResponse{Error: "origin is not allowed"})
		return controlplane.SourceConfig{}, false
	}
	h.auditAcceptedQueryToken(ctx, decision, source)
	return source, true
}

func (h *Handler) auditAcceptedQueryToken(ctx fiber.Ctx, decision queryTokenAuthDecision, source controlplane.SourceConfig) {
	if decision.State != queryTokenAuthAllowedGrace {
		return
	}
	log.Printf(
		"accepted rotated query token token_id=%s tenant_id=%s project_id=%s source_id=%s route=%s remote_ip=%s",
		decision.Credential.ID,
		source.TenantID,
		source.ProjectID,
		source.SourceID,
		ctx.Path(),
		h.clientIP(ctx),
	)
}

func (h *Handler) auditRejectedQueryToken(ctx fiber.Ctx, decision queryTokenAuthDecision) {
	switch decision.State {
	case queryTokenAuthExpired:
		log.Printf("rejected expired query token token_id=%s route=%s remote_ip=%s", decision.Credential.ID, ctx.Path(), h.clientIP(ctx))
	case queryTokenAuthNotYetValid:
		log.Printf("rejected not-yet-valid query token token_id=%s route=%s remote_ip=%s", decision.Credential.ID, ctx.Path(), h.clientIP(ctx))
	default:
		log.Printf("rejected unknown query token route=%s remote_ip=%s", ctx.Path(), h.clientIP(ctx))
	}
}

func (h *Handler) queryWriteKey(ctx fiber.Ctx) string {
	if value := strings.TrimSpace(ctx.Get("X-SimpleTrack-Write-Key")); value != "" {
		return value
	}
	return strings.TrimSpace(ctx.Query("write_key"))
}

func (h *Handler) writeQueryError(ctx fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, storage.ErrInvalidEventQuery):
		return h.writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: err.Error()})
	default:
		log.Printf("query failed: %v", err)
		return h.writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
	}
}

func parseRequiredQueryTime(ctx fiber.Ctx, key string) (time.Time, error) {
	value := strings.TrimSpace(ctx.Query(key))
	if value == "" {
		return time.Time{}, errors.New(key + " is required")
	}
	return parseTimeValue(value)
}

func parseQueryTimeOrDefault(ctx fiber.Ctx, key string, fallback time.Time) (time.Time, error) {
	value := strings.TrimSpace(ctx.Query(key))
	if value == "" {
		return fallback.UTC(), nil
	}
	return parseTimeValue(value)
}

func parseQueryLimitOrDefault(ctx fiber.Ctx, key string, fallback int) (int, error) {
	return normalizeQueryIntOrDefault(ctx, key, fallback, 1)
}

func parseQueryIntOrDefault(ctx fiber.Ctx, key string, fallback int) (int, error) {
	return normalizeQueryIntOrDefault(ctx, key, fallback, 0)
}

func normalizeQueryIntOrDefault(ctx fiber.Ctx, key string, fallback int, min int) (int, error) {
	value := strings.TrimSpace(ctx.Query(key))
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

func parsePropertyFilters(ctx fiber.Ctx, source controlplane.SourceConfig) ([]storage.EventPropertyFilter, error) {
	// Property filters are repeatable JSON query parameters. Keep the shape
	// explicit so future UI filters do not invent ad hoc dynamic SQL fragments.
	rawFilters := propertyFilterArgs(ctx)
	if len(rawFilters) == 0 {
		return nil, nil
	}
	if len(rawFilters) > defaultPropertyFilterCap {
		return nil, invalidPropertyFilterError("too many property filters: %d > %d", len(rawFilters), defaultPropertyFilterCap)
	}
	filters := make([]storage.EventPropertyFilter, 0, len(rawFilters))
	for idx, raw := range rawFilters {
		// Parse and validate each filter independently so the public error can
		// point at the failing filter index while never reaching EventReader.
		filter, err := parsePropertyFilter(idx, raw, source)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

func parsePropertyCatalogScope(ctx fiber.Ctx) (storage.PropertyScope, error) {
	value := strings.ToLower(strings.TrimSpace(ctx.Query("scope")))
	if value == "" {
		return "", nil
	}
	if value != string(storage.PropertyScopeEvent) && value != string(storage.PropertyScopeUser) {
		return "", errors.New("scope must be event or user")
	}
	return storage.PropertyScope(value), nil
}

func parsePropertyCatalogLimit(ctx fiber.Ctx) (int, error) {
	limit, err := parseQueryLimitOrDefault(ctx, "limit", defaultPropertyCatalogLimit)
	if err != nil {
		return 0, err
	}
	if limit > maxPropertyCatalogLimit {
		return 0, fmt.Errorf("limit must be less than or equal to %d", maxPropertyCatalogLimit)
	}
	return limit, nil
}

func propertyFilterArgs(ctx fiber.Ctx) [][]byte {
	filters := make([][]byte, 0, 2)
	ctx.Request().URI().QueryArgs().VisitAll(func(key []byte, value []byte) {
		if string(key) != "property_filter" {
			return
		}
		copied := append([]byte(nil), value...)
		filters = append(filters, copied)
	})
	return filters
}

func parsePropertyFilter(idx int, raw []byte, source controlplane.SourceConfig) (storage.EventPropertyFilter, error) {
	// Decode the URL-decoded JSON payload first. Query callers can repeat the
	// parameter, but each value has one small schema with bound scalar values.
	var payload propertyFilterPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return storage.EventPropertyFilter{}, invalidPropertyFilterError("property filter %d must be JSON", idx)
	}
	scope := strings.ToLower(strings.TrimSpace(payload.Scope))
	name := strings.TrimSpace(payload.Name)
	valueType := strings.ToLower(strings.TrimSpace(payload.Type))
	operator := strings.ToLower(strings.TrimSpace(payload.Operator))
	if operator == "" {
		operator = string(storage.EventFilterEquals)
	}
	if scope == "" || name == "" || valueType == "" {
		return storage.EventPropertyFilter{}, invalidPropertyFilterError("property filter %d scope, name, and type are required", idx)
	}
	if operator != string(storage.EventFilterEquals) && operator != string(storage.EventFilterNotEquals) {
		return storage.EventPropertyFilter{}, invalidPropertyFilterError("property filter %d unsupported operator %q", idx, operator)
	}

	// Enforce the SaaS-owned runtime source whitelist before analytics-core
	// builds any storage query. The ClickHouse builder receives the startup
	// selector surface as a second fail-closed guard.
	if !source.AllowsPropertyFilter(scope, name, valueType) {
		return storage.EventPropertyFilter{}, invalidPropertyFilterError("property filter %d %s.%s %s is not allowlisted", idx, scope, name, valueType)
	}

	filter := storage.EventPropertyFilter{
		Scope:     storage.PropertyScope(scope),
		Name:      name,
		ValueType: storage.PropertyValueType(valueType),
		Operator:  storage.EventFilterOperator(operator),
	}
	if err := assignPropertyFilterValue(&filter, payload.Value); err != nil {
		return storage.EventPropertyFilter{}, invalidPropertyFilterError("property filter %d %v", idx, err)
	}
	return filter, nil
}

func assignPropertyFilterValue(filter *storage.EventPropertyFilter, value any) error {
	// Copy the JSON scalar into the typed analytics-core slot. Nested values and
	// stringified numbers are rejected so query behavior matches collect-time
	// typed property indexing.
	switch filter.ValueType {
	case storage.PropertyValueNull:
		return nil
	case storage.PropertyValueString:
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("string value must be a JSON string")
		}
		filter.StringValue = text
	case storage.PropertyValueNumber:
		number, ok := value.(float64)
		if !ok {
			return fmt.Errorf("number value must be a JSON number")
		}
		filter.NumberValue = number
	case storage.PropertyValueBool:
		boolean, ok := value.(bool)
		if !ok {
			return fmt.Errorf("bool value must be a JSON boolean")
		}
		filter.BoolValue = boolean
	default:
		return fmt.Errorf("unsupported property value type %q", filter.ValueType)
	}
	return nil
}

func invalidPropertyFilterError(format string, args ...any) error {
	args = append([]any{storage.ErrInvalidEventQuery}, args...)
	return fmt.Errorf("%w: "+format, args...)
}

func toPropertySelectors(filters []controlplane.AllowedPropertyFilter) []storage.PropertySelector {
	if len(filters) == 0 {
		return nil
	}
	selectors := make([]storage.PropertySelector, 0, len(filters))
	seen := make(map[storage.PropertySelector]struct{}, len(filters))
	for _, filter := range filters {
		// Carry only the selector into analytics-core. The service has already
		// checked value-type restrictions against the source runtime config.
		selector := storage.PropertySelector{
			Scope: storage.PropertyScope(strings.ToLower(strings.TrimSpace(filter.Scope))),
			Name:  strings.TrimSpace(filter.Name),
		}
		if selector.Scope == "" || selector.Name == "" {
			continue
		}
		if _, ok := seen[selector]; ok {
			continue
		}
		seen[selector] = struct{}{}
		selectors = append(selectors, selector)
	}
	return selectors
}

func toPropertyCatalogItemResponses(entries []storage.PropertyCatalogEntry) []propertyCatalogItemResponse {
	responses := make([]propertyCatalogItemResponse, 0, len(entries))
	for _, entry := range entries {
		responses = append(responses, propertyCatalogItemResponse{
			Scope:       string(entry.Scope),
			Name:        entry.Name,
			ValueType:   string(entry.ValueType),
			FirstSeenAt: entry.FirstSeenAt.UTC().Format(time.RFC3339Nano),
			LastSeenAt:  entry.LastSeenAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return responses
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

// toQueryEvidenceResponse converts analytics-core evidence into the internal API shape.
func toQueryEvidenceResponse(evidence storage.EventQueryEvidence) *queryEvidenceResponse {
	if evidence.Family == "" {
		return nil
	}
	return &queryEvidenceResponse{
		Family:              string(evidence.Family),
		ReadPath:            string(evidence.ReadPath),
		Optimization:        string(evidence.Optimization),
		EffectiveLimit:      evidence.EffectiveLimit,
		Offset:              evidence.Offset,
		HasTimeLowerBound:   evidence.HasTimeLowerBound,
		HasTimeUpperBound:   evidence.HasTimeUpperBound,
		TimeWindowSeconds:   evidence.TimeWindowSeconds,
		ScalarFilterCount:   evidence.ScalarFilterCount,
		PropertyFilterCount: evidence.PropertyFilterCount,
		UsesPropertyTable:   evidence.UsesPropertyTable,
		SortField:           string(evidence.SortField),
		SortDirection:       string(evidence.SortDirection),
		Pressure:            queryPressure(evidence),
	}
}

// queryPressure assigns a coarse read-side triage bucket.
//
// The bucket is intentionally simple: it helps operators compare query shapes
// and decide whether a request deserves follow-up optimization work, but it is
// not a hard latency SLA or an automatic scaling trigger.
func queryPressure(evidence storage.EventQueryEvidence) string {
	totalFilters := evidence.ScalarFilterCount + evidence.PropertyFilterCount
	switch {
	case evidence.PropertyFilterCount == 0 && evidence.ScalarFilterCount <= 2:
		return pressureLow
	case evidence.PropertyFilterCount <= 2 && totalFilters <= 6:
		return pressureMedium
	default:
		return pressureHigh
	}
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
		VisitID:        record.VisitID,
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
