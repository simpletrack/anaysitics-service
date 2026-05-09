package controlplane

import (
	"context"
	"errors"
	"strings"
	"time"
)

const defaultVisitWindow = 30 * time.Minute

// ErrSourceNotFound reports an unknown write key.
var ErrSourceNotFound = errors.New("analytics source not found")

// ErrSourceDisabled reports a known source that is currently disabled.
var ErrSourceDisabled = errors.New("analytics source is disabled")

// ErrSourceOutsideSchemaSurface reports a source not present in the boot-time schema surface.
var ErrSourceOutsideSchemaSurface = errors.New("analytics source is outside ingestion schema surface")

// SourceConfig is the runtime-only view of a SimpleTrack analytics source.
//
// The SaaS control plane owns the lifecycle of these values. The analytics
// service only reads them to enforce runtime collection rules.
type SourceConfig struct {
	WriteKey                 string                  `json:"write_key"`                  // WriteKey is the public runtime key accepted by /collect
	Enabled                  bool                    `json:"enabled"`                    // Enabled controls whether the source may accept runtime events
	TenantID                 string                  `json:"tenant_id"`                  // TenantID maps the workspace boundary into analytics-core
	ProjectID                string                  `json:"project_id"`                 // ProjectID maps the site or project boundary into analytics-core
	SourceID                 string                  `json:"source_id"`                  // SourceID identifies the concrete event source inside the project
	SourceType               string                  `json:"source_type"`                // SourceType is the analytics-core source category, usually web
	AllowedOrigins           []string                `json:"allowed_origins"`            // AllowedOrigins are browser origins allowed to send events for this source
	AllowedPropertyFilters   []AllowedPropertyFilter `json:"allowed_property_filters"`   // AllowedPropertyFilters are source-scoped typed property query selectors
	ReadbackPolicy           ReadbackPolicy          `json:"readback_policy"`            // ReadbackPolicy gates internal readback route families for this source
	BotUserAgents            []string                `json:"bot_user_agents"`            // BotUserAgents override analytics-core default bot user-agent tokens
	InternalCIDRs            []string                `json:"internal_cidrs"`             // InternalCIDRs are runtime network ranges filtered before publishing
	InternalIPs              []string                `json:"internal_ips"`               // InternalIPs are exact runtime client addresses filtered before publishing
	SessionSalt              string                  `json:"session_salt"`               // SessionSalt namespaces derived session ids
	VisitSalt                string                  `json:"visit_salt"`                 // VisitSalt namespaces derived canonical analytics visit ids
	VisitWindow              time.Duration           `json:"-"`                          // VisitWindow is the server-side visit bucket used by collect stages
	VisitWindowSeconds       int                     `json:"visit_window_seconds"`       // VisitWindowSeconds encodes VisitWindow for JSON runtime configs
	ClientHashSalt           string                  `json:"client_hash_salt"`           // ClientHashSalt namespaces derived client IP hashes
	IncludeClientFingerprint bool                    `json:"include_client_fingerprint"` // IncludeClientFingerprint adds transient client data to derived sessions
}

// ReadbackRoute identifies one internal analytics readback route family.
type ReadbackRoute string

const (
	// ReadbackRouteRealtime gates recent-event readback used by Realtime views.
	ReadbackRouteRealtime ReadbackRoute = "realtime"
	// ReadbackRouteEvents gates historical event-list readback used by Events views.
	ReadbackRouteEvents ReadbackRoute = "events"
	// ReadbackRouteProperties gates property-catalog readback used by filter builders.
	ReadbackRouteProperties ReadbackRoute = "properties"
	// ReadbackRouteGoals gates P1 goal-count readback used by goal status cards.
	ReadbackRouteGoals ReadbackRoute = "goals"
)

// ReadbackPolicy describes which internal readback families a source may use.
//
// The zero value denies every route. This is intentional: missing control-plane
// policy must fail closed instead of silently exposing analytics reads.
type ReadbackPolicy struct {
	Realtime   bool `json:"realtime"`   // Realtime allows /v1/realtime readback for this source
	Events     bool `json:"events"`     // Events allows /v1/events readback for this source
	Properties bool `json:"properties"` // Properties allows /v1/properties metadata readback for this source
	Goals      bool `json:"goals"`      // Goals allows /v1/goals summary readback for this source
}

// AllowedPropertyFilter describes one source-scoped property selector allowed in Events readback.
type AllowedPropertyFilter struct {
	Scope      string   `json:"scope"`       // Scope is event or user
	Name       string   `json:"name"`        // Name is the normalized property key allowed for filtering
	ValueTypes []string `json:"value_types"` // ValueTypes optionally restrict the allowed typed property slots
}

// Normalize returns a copy with stable defaults and trimmed string fields.
func (c SourceConfig) Normalize() SourceConfig {
	c.WriteKey = strings.TrimSpace(c.WriteKey)
	c.TenantID = strings.TrimSpace(c.TenantID)
	c.ProjectID = strings.TrimSpace(c.ProjectID)
	c.SourceID = strings.TrimSpace(c.SourceID)
	c.SourceType = strings.TrimSpace(c.SourceType)
	c.SessionSalt = strings.TrimSpace(c.SessionSalt)
	c.VisitSalt = strings.TrimSpace(c.VisitSalt)
	c.ClientHashSalt = strings.TrimSpace(c.ClientHashSalt)
	if c.SourceType == "" {
		c.SourceType = "web"
	}
	c.VisitWindow = normalizeVisitWindow(c.VisitWindow, c.VisitWindowSeconds)
	c.VisitWindowSeconds = int(c.VisitWindow / time.Second)
	c.AllowedOrigins = normalizeStringList(c.AllowedOrigins)
	c.AllowedPropertyFilters = normalizeAllowedPropertyFilters(c.AllowedPropertyFilters)
	c.BotUserAgents = normalizeStringList(c.BotUserAgents)
	c.InternalCIDRs = normalizeStringList(c.InternalCIDRs)
	c.InternalIPs = normalizeStringList(c.InternalIPs)
	return c
}

// AllowsOrigin reports whether origin may send browser events for this source.
func (c SourceConfig) AllowsOrigin(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	for _, allowed := range c.AllowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return true
		}
	}
	return false
}

// AllowsPropertyFilter reports whether a typed property predicate is permitted for this source.
func (c SourceConfig) AllowsPropertyFilter(scope string, name string, valueType string) bool {
	// Normalize request-side strings exactly like source config normalization so
	// callers cannot bypass the allowlist with whitespace or case drift.
	scope = strings.ToLower(strings.TrimSpace(scope))
	name = strings.TrimSpace(name)
	valueType = strings.ToLower(strings.TrimSpace(valueType))
	if scope == "" || name == "" || valueType == "" {
		return false
	}

	// Match by source-owned selector first, then optionally restrict the typed
	// value slot. An empty ValueTypes list means any scalar property type is
	// allowed for that selector.
	for _, filter := range c.AllowedPropertyFilters {
		if filter.Scope != scope || filter.Name != name {
			continue
		}
		if len(filter.ValueTypes) == 0 {
			return true
		}
		for _, allowed := range filter.ValueTypes {
			if allowed == valueType {
				return true
			}
		}
	}
	return false
}

// AllowsReadback reports whether source policy permits one internal read route.
func (c SourceConfig) AllowsReadback(route ReadbackRoute) bool {
	// Keep route gating centralized in the control-plane model. HTTP handlers
	// should not infer defaults or duplicate policy switches per endpoint.
	switch route {
	case ReadbackRouteRealtime:
		return c.ReadbackPolicy.Realtime
	case ReadbackRouteEvents:
		return c.ReadbackPolicy.Events
	case ReadbackRouteProperties:
		return c.ReadbackPolicy.Properties
	case ReadbackRouteGoals:
		return c.ReadbackPolicy.Goals
	default:
		return false
	}
}

// Resolver resolves runtime source configuration from a write key.
type Resolver interface {
	// ResolveSource returns the source config used to enforce one collect request.
	ResolveSource(context.Context, string) (SourceConfig, error)
}

// MemoryResolver resolves source configuration from an in-memory map.
//
// It is intended for local development and tests. Production should replace it
// with a resolver backed by the SimpleTrack SaaS control-plane database, API,
// or cache.
type MemoryResolver struct {
	sources map[string]SourceConfig // sources stores normalized configs by write key
}

// SchemaBoundResolver rejects resolved sources outside a boot-time schema surface.
//
// It is used when the runtime service resolves source config through a dynamic
// control-plane API while same-process ingestion still validates ClickHouse
// routed tables at startup. The wrapper prevents collect from accepting a source
// that was never part of startup schema validation.
type SchemaBoundResolver struct {
	inner Resolver                      // inner resolves runtime source config from memory or HTTP
	allow map[schemaSurfaceKey]struct{} // allow records enabled sources validated at service startup
}

type schemaSurfaceKey struct {
	writeKey   string // writeKey ties the public runtime key to the schema surface
	tenantID   string // tenantID is the analytics-core tenant boundary
	projectID  string // projectID is the analytics-core project boundary
	sourceID   string // sourceID is the analytics-core source boundary
	sourceType string // sourceType prevents routing a different source family through this schema
}

// NewSchemaBoundResolver wraps resolver with boot-time source schema checks.
func NewSchemaBoundResolver(resolver Resolver, sources []SourceConfig) (*SchemaBoundResolver, error) {
	if resolver == nil {
		return nil, errors.New("control-plane resolver is required")
	}
	allow := make(map[schemaSurfaceKey]struct{}, len(sources))
	for _, source := range sources {
		// Only enabled sources get routed ClickHouse tables at startup; disabled
		// entries must not accidentally authorize collect-time writes.
		source = source.Normalize()
		if !source.Enabled {
			continue
		}
		if err := validateSourceConfig(source); err != nil {
			return nil, err
		}
		allow[newSchemaSurfaceKey(source)] = struct{}{}
	}
	return &SchemaBoundResolver{inner: resolver, allow: allow}, nil
}

// ResolveSource returns a source only when it was present at startup.
func (r *SchemaBoundResolver) ResolveSource(ctx context.Context, writeKey string) (SourceConfig, error) {
	if r == nil {
		return SourceConfig{}, errors.New("control-plane resolver is required")
	}

	// Resolve first so the control plane remains authoritative for enabled
	// state, salts, domain rules, and source identity; then gate the answer
	// against startup schema validation before /collect can publish.
	source, err := r.inner.ResolveSource(ctx, writeKey)
	if err != nil {
		return SourceConfig{}, err
	}
	source = source.Normalize()
	if _, ok := r.allow[newSchemaSurfaceKey(source)]; !ok {
		return SourceConfig{}, ErrSourceOutsideSchemaSurface
	}
	return source, nil
}

// NewMemoryResolver creates a resolver from static source configs.
func NewMemoryResolver(sources []SourceConfig) (*MemoryResolver, error) {
	index := make(map[string]SourceConfig, len(sources))
	for _, source := range sources {
		// Normalize first so validation and duplicate checks use the same write
		// key that runtime collect requests will resolve.
		source = source.Normalize()
		if err := validateSourceConfig(source); err != nil {
			return nil, err
		}
		if _, exists := index[source.WriteKey]; exists {
			return nil, errors.New("source write key must be unique")
		}

		// Index by write key only after all boundary fields are proven valid. A
		// bad config must fail startup instead of rerouting accepted events.
		index[source.WriteKey] = source
	}
	return &MemoryResolver{sources: index}, nil
}

// ResolveSource returns one configured source by write key.
func (r *MemoryResolver) ResolveSource(_ context.Context, writeKey string) (SourceConfig, error) {
	// Reject missing runtime dependencies as a server-side configuration error;
	// callers convert this to a stable public response.
	if r == nil {
		return SourceConfig{}, errors.New("control-plane resolver is required")
	}

	// Resolve against the normalized key exactly as provided to /collect. Unknown
	// or disabled sources are distinct so the HTTP layer can choose status codes
	// without exposing tenant/project/source details.
	source, ok := r.sources[strings.TrimSpace(writeKey)]
	if !ok {
		return SourceConfig{}, ErrSourceNotFound
	}
	if !source.Enabled {
		return SourceConfig{}, ErrSourceDisabled
	}
	return source, nil
}

func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func normalizeAllowedPropertyFilters(values []AllowedPropertyFilter) []AllowedPropertyFilter {
	out := make([]AllowedPropertyFilter, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		// Normalize selector shape but keep validation separate so startup can
		// report unsupported scopes or value types as configuration errors.
		value.Scope = strings.ToLower(strings.TrimSpace(value.Scope))
		value.Name = strings.TrimSpace(value.Name)
		value.ValueTypes = normalizeLowerStringList(value.ValueTypes)
		if value.Scope == "" || value.Name == "" {
			continue
		}
		key := value.Scope + "\x00" + value.Name + "\x00" + strings.Join(value.ValueTypes, ",")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeLowerStringList(values []string) []string {
	out := normalizeStringList(values)
	for idx := range out {
		out[idx] = strings.ToLower(out[idx])
	}
	return out
}

func normalizeVisitWindow(window time.Duration, encodedSeconds int) time.Duration {
	if window <= 0 && encodedSeconds > 0 {
		window = time.Duration(encodedSeconds) * time.Second
	}
	if window <= 0 {
		return defaultVisitWindow
	}
	return window
}

func newSchemaSurfaceKey(source SourceConfig) schemaSurfaceKey {
	return schemaSurfaceKey{
		writeKey:   source.WriteKey,
		tenantID:   source.TenantID,
		projectID:  source.ProjectID,
		sourceID:   source.SourceID,
		sourceType: source.SourceType,
	}
}

func validateSourceConfig(source SourceConfig) error {
	// Validate the privacy and routing fields required before a source can be
	// indexed, cached, or used for collect-time trust-boundary overrides.
	if source.WriteKey == "" {
		return errors.New("source write key is required")
	}
	if source.TenantID == "" {
		return errors.New("source tenant id is required")
	}
	if source.ProjectID == "" {
		return errors.New("source project id is required")
	}
	if source.SourceID == "" {
		return errors.New("source id is required")
	}
	if source.SessionSalt == "" {
		return errors.New("source session salt is required")
	}
	if source.VisitSalt == "" {
		return errors.New("source visit salt is required")
	}
	if source.VisitWindow < time.Minute {
		return errors.New("source visit window must be at least one minute")
	}
	if source.ClientHashSalt == "" {
		return errors.New("source client hash salt is required")
	}
	if err := validateAllowedPropertyFilters(source.AllowedPropertyFilters); err != nil {
		return err
	}
	return nil
}

func validateAllowedPropertyFilters(filters []AllowedPropertyFilter) error {
	for _, filter := range filters {
		if filter.Scope != "event" && filter.Scope != "user" {
			return errors.New("source property filter scope must be event or user")
		}
		for _, valueType := range filter.ValueTypes {
			switch valueType {
			case "null", "string", "number", "bool":
			default:
				return errors.New("source property filter value type must be null, string, number, or bool")
			}
		}
	}
	return nil
}
