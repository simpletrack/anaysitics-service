# simpletrack-analytics-service

`simpletrack-analytics-service` is the runtime analytics data-plane service for
SimpleTrack.

It is intentionally separate from the SaaS control plane. The SaaS application
owns users, organizations, sites, write-key lifecycle, domain allowlists,
subscription limits, permissions, and dashboards. This service only reads the
runtime view of that configuration, enforces it on hot-path requests, and calls
`github.com/simpletrack/analytics-core` as a Go library.

## Responsibilities

- Serve `GET /healthz` for process health.
- Serve `GET /tracker.js` for the P1 browser tracker asset.
- Serve `OPTIONS /collect` for browser preflight.
- Serve `POST /collect` for browser and server event intake.
- Resolve write keys to runtime source configuration.
- Override untrusted client-supplied tenant, project, source, and source type.
- Enforce source enabled state and origin allowlists before analytics-core sees the event.
- Apply analytics-core collect stages for bot filtering, internal traffic filtering, client enrichment, and session derivation.

## Non-Responsibilities

- It does not create or update users, organizations, sites, write keys, plans, subscriptions, or permissions.
- It does not own SaaS dashboard pages.
- It does not replace `simpletrack-saas`.
- It does not make `analytics-core` a deployable business service.

## Local Run

Set one local source config:

```powershell
$env:ANALYTICS_SERVICE_REDIS_ADDR='127.0.0.1:26379'
$env:ANALYTICS_SERVICE_SOURCES_JSON='[
  {
    "write_key":"wk_local",
    "enabled":true,
    "tenant_id":"tenant_local",
    "project_id":"project_local",
    "source_id":"source_web",
    "source_type":"web",
    "allowed_origins":["http://localhost:3000"],
    "session_salt":"local-session-salt",
    "client_hash_salt":"local-client-salt",
    "include_client_fingerprint":true
  }
]'
go run ./cmd/analytics-service
```

For a throwaway demo without Redis, opt into the non-durable in-memory queue:

```powershell
$env:ANALYTICS_SERVICE_EVENTBUS='direct'
$env:ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS='true'
```

Send a collect request:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/collect `
  -ContentType application/json `
  -Headers @{ Origin = "http://localhost:3000" } `
  -Body '{"write_key":"wk_local","id":"evt_1","event_name":"pageview","distinct_id":"visitor_1"}'
```

## Verification

```powershell
go test ./...
go vet ./...
```
