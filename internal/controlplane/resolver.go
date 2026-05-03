package controlplane

import (
	"context"
	"errors"
	"strings"
)

// ErrSourceNotFound reports an unknown write key.
var ErrSourceNotFound = errors.New("analytics source not found")

// ErrSourceDisabled reports a known source that is currently disabled.
var ErrSourceDisabled = errors.New("analytics source is disabled")

// SourceConfig is the runtime-only view of a SimpleTrack analytics source.
//
// The SaaS control plane owns the lifecycle of these values. The analytics
// service only reads them to enforce runtime collection rules.
type SourceConfig struct {
	WriteKey                 string   `json:"write_key"`                  // WriteKey is the public runtime key accepted by /collect
	Enabled                  bool     `json:"enabled"`                    // Enabled controls whether the source may accept runtime events
	TenantID                 string   `json:"tenant_id"`                  // TenantID maps the workspace boundary into analytics-core
	ProjectID                string   `json:"project_id"`                 // ProjectID maps the site or project boundary into analytics-core
	SourceID                 string   `json:"source_id"`                  // SourceID identifies the concrete event source inside the project
	SourceType               string   `json:"source_type"`                // SourceType is the analytics-core source category, usually web
	AllowedOrigins           []string `json:"allowed_origins"`            // AllowedOrigins are browser origins allowed to send events for this source
	BotUserAgents            []string `json:"bot_user_agents"`            // BotUserAgents override analytics-core default bot user-agent tokens
	InternalCIDRs            []string `json:"internal_cidrs"`             // InternalCIDRs are runtime network ranges filtered before publishing
	InternalIPs              []string `json:"internal_ips"`               // InternalIPs are exact runtime client addresses filtered before publishing
	SessionSalt              string   `json:"session_salt"`               // SessionSalt namespaces derived session ids
	ClientHashSalt           string   `json:"client_hash_salt"`           // ClientHashSalt namespaces derived client IP hashes
	IncludeClientFingerprint bool     `json:"include_client_fingerprint"` // IncludeClientFingerprint adds transient client data to derived sessions
}

// Normalize returns a copy with stable defaults and trimmed string fields.
func (c SourceConfig) Normalize() SourceConfig {
	c.WriteKey = strings.TrimSpace(c.WriteKey)
	c.TenantID = strings.TrimSpace(c.TenantID)
	c.ProjectID = strings.TrimSpace(c.ProjectID)
	c.SourceID = strings.TrimSpace(c.SourceID)
	c.SourceType = strings.TrimSpace(c.SourceType)
	c.SessionSalt = strings.TrimSpace(c.SessionSalt)
	c.ClientHashSalt = strings.TrimSpace(c.ClientHashSalt)
	if c.SourceType == "" {
		c.SourceType = "web"
	}
	c.AllowedOrigins = normalizeStringList(c.AllowedOrigins)
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

// NewMemoryResolver creates a resolver from static source configs.
func NewMemoryResolver(sources []SourceConfig) (*MemoryResolver, error) {
	index := make(map[string]SourceConfig, len(sources))
	for _, source := range sources {
		// Normalize first so validation and duplicate checks use the same write
		// key that runtime collect requests will resolve.
		source = source.Normalize()
		if source.WriteKey == "" {
			return nil, errors.New("source write key is required")
		}
		if source.TenantID == "" {
			return nil, errors.New("source tenant id is required")
		}
		if source.ProjectID == "" {
			return nil, errors.New("source project id is required")
		}
		if source.SourceID == "" {
			return nil, errors.New("source id is required")
		}
		if source.SessionSalt == "" {
			return nil, errors.New("source session salt is required")
		}
		if source.ClientHashSalt == "" {
			return nil, errors.New("source client hash salt is required")
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
