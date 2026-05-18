package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxRuntimeConfigResponseBytes = 64 << 10

// HTTPResolverOptions configures HTTPResolver.
type HTTPResolverOptions struct {
	Endpoint              string           // Endpoint receives POST write-key resolution requests
	BearerToken           string           // BearerToken authenticates this runtime service to the SaaS control plane
	Timeout               time.Duration    // Timeout bounds each control-plane request when Client is not provided
	CacheTTL              time.Duration    // CacheTTL caches successful source configs to reduce hot-path control-plane load
	AllowInsecureLoopback bool             // AllowInsecureLoopback permits http:// loopback endpoints for local development only
	AllowInsecurePrivateNetwork bool       // AllowInsecurePrivateNetwork permits explicit http:// container-network endpoints for local Docker development only
	Now                   func() time.Time // Now returns the current time for cache expiry, primarily for tests
	Client                *http.Client     // Client overrides the default HTTP client, primarily for tests
}

// HTTPResolver resolves source config through the SaaS control-plane runtime API.
//
// The resolver is read-only. It never creates or mutates sources, write keys,
// quotas, domains, or privacy salts; it only retrieves the runtime view needed
// by /collect enforcement.
type HTTPResolver struct {
	endpoint    string        // endpoint is the validated control-plane resolution URL
	bearerToken string        // bearerToken is sent only to the configured control-plane endpoint
	client      *http.Client  // client performs bounded control-plane requests
	cacheTTL    time.Duration // cacheTTL limits how long a successful source config can be reused
	now         func() time.Time

	mu    sync.Mutex
	cache map[string]cachedSourceConfig
}

type cachedSourceConfig struct {
	source    SourceConfig // source is the normalized runtime config returned to /collect
	etag      string       // etag revalidates mutable runtime policy on subsequent lookups
	expiresAt time.Time    // expiresAt is the exclusive cache deadline
}

type resolveSourceRequest struct {
	WriteKey string `json:"write_key"` // WriteKey is the public key presented to /collect
}

// NewHTTPResolver creates a resolver backed by a control-plane HTTP endpoint.
func NewHTTPResolver(options HTTPResolverOptions) (*HTTPResolver, error) {
	// Validate the configured endpoint before constructing a client because this
	// path carries bearer tokens and server-only privacy salts.
	endpoint := strings.TrimSpace(options.Endpoint)
	if endpoint == "" {
		return nil, errors.New("control-plane endpoint is required")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("control-plane endpoint %q is not a valid absolute URL", endpoint)
	}
	if err := validateControlPlaneTransport(parsed, options.AllowInsecureLoopback, options.AllowInsecurePrivateNetwork); err != nil {
		return nil, err
	}

	// Require service-to-service authentication up front. Missing auth is a
	// deployment error, not a collect-time fallback to permissive behavior.
	bearerToken := strings.TrimSpace(options.BearerToken)
	if bearerToken == "" {
		return nil, errors.New("control-plane bearer token is required")
	}

	// Use a bounded, no-redirect client so Authorization never follows a 30x to
	// an unconfigured host or plaintext URL.
	now := options.Now
	if now == nil {
		now = time.Now
	}
	client := controlPlaneClient(options)
	return &HTTPResolver{
		endpoint:    endpoint,
		bearerToken: bearerToken,
		client:      client,
		cacheTTL:    options.CacheTTL,
		now:         now,
		cache:       make(map[string]cachedSourceConfig),
	}, nil
}

func controlPlaneClient(options HTTPResolverOptions) *http.Client {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if options.Client == nil {
		return &http.Client{
			Timeout:       timeout,
			CheckRedirect: rejectControlPlaneRedirect,
		}
	}

	// Clone the provided client value before installing redirect policy so tests
	// and callers do not see surprising mutation on their shared client.
	client := *options.Client
	if client.Timeout <= 0 {
		client.Timeout = timeout
	}
	client.CheckRedirect = rejectControlPlaneRedirect
	return &client
}

func rejectControlPlaneRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// ResolveSource returns the source config resolved by the SaaS control plane.
func (r *HTTPResolver) ResolveSource(ctx context.Context, writeKey string) (SourceConfig, error) {
	if r == nil {
		return SourceConfig{}, errors.New("control-plane resolver is required")
	}
	writeKey = strings.TrimSpace(writeKey)
	if writeKey == "" {
		return SourceConfig{}, ErrSourceNotFound
	}
	if cached, ok := r.cached(writeKey); ok {
		// Revalidate the cached runtime policy so disabled sources and rotated
		// salts/origins take effect without waiting for the local TTL to expire.
		source, etag, notModified, err := r.fetchSource(ctx, writeKey, cached.etag)
		if err != nil {
			return SourceConfig{}, err
		}
		if notModified {
			r.store(writeKey, cached.source, cached.etag)
			return cached.source, nil
		}
		r.store(writeKey, source, etag)
		return source, nil
	}

	// Fetch from the SaaS control plane only after cache miss. Any transport or
	// validation error fails closed and callers expose a stable 5xx response.
	source, etag, _, err := r.fetchSource(ctx, writeKey, "")
	if err != nil {
		return SourceConfig{}, err
	}
	r.store(writeKey, source, etag)
	return source, nil
}

func (r *HTTPResolver) cached(writeKey string) (cachedSourceConfig, bool) {
	// Cache lookups are intentionally success-only. Disabled or missing sources
	// must be rechecked so SaaS-side write-key state changes can take effect.
	if r.cacheTTL <= 0 {
		return cachedSourceConfig{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	cached, ok := r.cache[writeKey]
	// Treat expiresAt as an exclusive deadline. For example, with a cache entry
	// stored at 10:00:00 and CacheTTL=5s, a lookup at 10:00:04 may reuse it, but
	// a lookup at exactly 10:00:05 must miss and revalidate the control plane.
	if !ok || !r.now().Before(cached.expiresAt) {
		delete(r.cache, writeKey)
		return cachedSourceConfig{}, false
	}
	return cached, true
}

func (r *HTTPResolver) store(writeKey string, source SourceConfig, etag string) {
	// Store only normalized, validated source configs returned by fetchSource.
	// Each hit is revalidated with the control plane so mutable auth state does
	// not stay stale until the TTL expires.
	if r.cacheTTL <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[writeKey] = cachedSourceConfig{
		source:    source,
		etag:      strings.TrimSpace(etag),
		expiresAt: r.now().Add(r.cacheTTL),
	}
}

func (r *HTTPResolver) fetchSource(ctx context.Context, writeKey string, etag string) (SourceConfig, string, bool, error) {
	// Send only the public write key to the control plane. Tenant, project,
	// source, salts, and filtering rules come back from the trusted response.
	body, err := json.Marshal(resolveSourceRequest{WriteKey: writeKey})
	if err != nil {
		return SourceConfig{}, "", false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return SourceConfig{}, "", false, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+r.bearerToken)
	if etag = strings.TrimSpace(etag); etag != "" {
		request.Header.Set("If-None-Match", etag)
	}

	// Any transport failure leaves the request unresolved. The HTTP collect
	// layer converts this to a stable 5xx response instead of accepting an event
	// with unknown runtime configuration.
	response, err := r.client.Do(request)
	if err != nil {
		return SourceConfig{}, "", false, err
	}
	defer response.Body.Close()

	// Map stable control-plane statuses to collect-facing runtime errors without
	// exposing tenant, project, source, or write-key state.
	switch response.StatusCode {
	case http.StatusOK:
		source, err := decodeRuntimeSource(response.Body, writeKey)
		if err != nil {
			return SourceConfig{}, "", false, err
		}
		return source, strings.TrimSpace(response.Header.Get("ETag")), false, nil
	case http.StatusNotModified:
		return SourceConfig{}, strings.TrimSpace(response.Header.Get("ETag")), true, nil
	case http.StatusNotFound:
		return SourceConfig{}, "", false, ErrSourceNotFound
	case http.StatusForbidden, http.StatusGone:
		return SourceConfig{}, "", false, ErrSourceDisabled
	default:
		return SourceConfig{}, "", false, fmt.Errorf("control-plane resolver returned status %d", response.StatusCode)
	}
}

func decodeRuntimeSource(body io.Reader, writeKey string) (SourceConfig, error) {
	// Bound the response before decoding because this path runs on collect
	// traffic and should not allow an oversized control-plane response to tie up
	// memory before fail-closed validation.
	limited := io.LimitReader(body, maxRuntimeConfigResponseBytes)
	var source SourceConfig
	if err := json.NewDecoder(limited).Decode(&source); err != nil {
		return SourceConfig{}, err
	}
	source = source.Normalize()
	if source.WriteKey != writeKey {
		return SourceConfig{}, errors.New("control-plane response write key mismatch")
	}
	if err := validateSourceConfig(source); err != nil {
		return SourceConfig{}, err
	}
	if !source.Enabled {
		return SourceConfig{}, ErrSourceDisabled
	}
	return source, nil
}

func validateControlPlaneTransport(endpoint *url.URL, allowInsecureLoopback bool, allowInsecurePrivateNetwork bool) error {
	// Production control-plane reads carry bearer tokens and privacy salts, so
	// HTTPS is mandatory unless a developer explicitly chooses loopback HTTP.
	if endpoint.Scheme == "https" {
		return nil
	}
	if endpoint.Scheme != "http" {
		return fmt.Errorf("control-plane endpoint scheme %q is not supported", endpoint.Scheme)
	}
	if !allowInsecureLoopback {
		if !allowInsecurePrivateNetwork {
			return errors.New("ANALYTICS_SERVICE_CONTROL_PLANE_ALLOW_INSECURE_LOOPBACK=true or ANALYTICS_SERVICE_CONTROL_PLANE_ALLOW_INSECURE_PRIVATE_NETWORK=true is required for http control-plane endpoints")
		}
	}
	if !isLoopbackHost(endpoint.Host) {
		if allowInsecurePrivateNetwork && isPrivateNetworkHost(endpoint.Host) {
			return nil
		}
		return errors.New("insecure control-plane endpoints are allowed only on loopback hosts")
	}
	return nil
}

func isLoopbackHost(hostPort string) bool {
	// Accept localhost and loopback IP literals, including bracketed IPv6 with
	// optional ports, for local httptest/dev control-plane endpoints only.
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}

func isPrivateNetworkHost(hostPort string) bool {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err == nil {
		return addr.IsPrivate()
	}

	// Docker service names such as `saas` or `analytics-service` are internal-only
	// hostnames when used inside the local compose bridge network.
	return !strings.Contains(host, ".")
}
