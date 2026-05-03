# simpletrack-anaysitics-service

`simpletrack-anaysitics-service` is the runtime analytics data-plane service for
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
- Optionally run the Redis Stream ingestion worker that writes accepted events to ClickHouse through `analytics-core`.

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
    "allowed_property_filters":[
      {"scope":"event","name":"button","value_types":["string"]},
      {"scope":"user","name":"plan","value_types":["string"]}
    ],
    "session_salt":"local-session-salt",
    "client_hash_salt":"local-client-salt",
    "include_client_fingerprint":true
  }
]'
go run ./cmd/simpletrack-anaysitics-service
```

`session_salt` and `client_hash_salt` are server-only runtime secrets. They must
come from the control plane or local runtime config and must not be derived from
the public write key shown in a browser snippet.

For production-shaped runtime config reads, switch source resolution to the SaaS
control-plane HTTP endpoint:

```powershell
$env:ANALYTICS_SERVICE_SOURCE_RESOLVER='http'
$env:ANALYTICS_SERVICE_CONTROL_PLANE_URL='https://saas.example.com/internal/analytics/runtime-source'
$env:ANALYTICS_SERVICE_CONTROL_PLANE_TOKEN='runtime-service-token'
$env:ANALYTICS_SERVICE_CONTROL_PLANE_TIMEOUT='3s'
$env:ANALYTICS_SERVICE_CONTROL_PLANE_CACHE_TTL='5s'
```

Control-plane URLs must use HTTPS by default because the request carries the
runtime bearer token and the response carries server-only privacy salts. Local
development may use loopback HTTP only with
`ANALYTICS_SERVICE_CONTROL_PLANE_ALLOW_INSECURE_LOOPBACK=true`.

The service sends only the write key to that endpoint and expects a runtime
`SourceConfig` response. It still does not create sources, rotate write keys, or
own domain/quota configuration. Successful responses can be cached for the short
TTL, but each cache hit is conditionally revalidated with `If-None-Match` when
the SaaS endpoint returns an `ETag`; disabled sources, salt rotation, and origin
changes therefore fail closed instead of waiting for stale local cache expiry.
If same-process ingestion is enabled, `ANALYTICS_SERVICE_SOURCES_JSON` is still
required as the startup schema surface for enabled sources, and HTTP-resolved
sources outside that startup surface are rejected.

When query mode is enabled, the service also exposes internal readback routes:

- `GET /v1/realtime`
- `GET /v1/events`

These routes require an internal bearer token and use `X-SimpleTrack-Write-Key`
or `write_key` to resolve the source boundary before calling
`analytics-core/storage.EventReader`. Production dashboard readback should stay
server-side in `simpletrack-saas` so the internal bearer token never reaches the
browser. `OPTIONS /v1/realtime` and `OPTIONS /v1/events` exist for protocol
completeness and trusted service/browser-shell integrations, but the actual GET
request still requires the bearer token and source allowlist check.

```powershell
$env:ANALYTICS_SERVICE_QUERY_ENABLED='true'
$env:ANALYTICS_SERVICE_QUERY_TOKEN='query-service-token'
$env:ANALYTICS_SERVICE_CLICKHOUSE_ADDR='127.0.0.1:29000'
$env:ANALYTICS_SERVICE_CLICKHOUSE_DATABASE='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_USER='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_PASSWORD='analytics_core'

Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/realtime?write_key=wk_local' `
  -Headers @{ Authorization = "Bearer query-service-token" }

Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/events?write_key=wk_local&from=2026-05-03T00:00:00Z&to=2026-05-04T00:00:00Z' `
  -Headers @{ Authorization = "Bearer query-service-token" }
```

Events readback also accepts repeatable `property_filter` query parameters. Each
value is URL-encoded JSON, and each selector must appear in the source's
`allowed_property_filters` runtime config before the request reaches
`analytics-core`:

```powershell
$filter = [uri]::EscapeDataString('{"scope":"event","name":"button","type":"string","op":"eq","value":"hero"}')
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8080/v1/events?write_key=wk_local&from=2026-05-03T00:00:00Z&to=2026-05-04T00:00:00Z&property_filter=$filter" `
  -Headers @{ Authorization = "Bearer query-service-token" }
```

For query token rotation, keep `ANALYTICS_SERVICE_QUERY_TOKEN` as the current
token and temporarily add the previous token to
`ANALYTICS_SERVICE_QUERY_TOKENS_JSON`:

```powershell
$env:ANALYTICS_SERVICE_QUERY_TOKEN='query-service-token-v2'
$env:ANALYTICS_SERVICE_QUERY_TOKENS_JSON='["query-service-token-v1"]'
```

Rotation should be short-lived: deploy the service accepting both tokens,
switch `simpletrack-saas` to the new token, then remove the old token from the
JSON allowlist once all callers have moved. This service only enforces accepted
runtime tokens; token creation, storage, and operator workflow remain owned by
deployment and the SaaS control-plane environment.

By default the service only accepts `/collect` and durably enqueues events to Redis.
To run ingestion in the same process for local or small deployments, opt in:

```powershell
$env:ANALYTICS_SERVICE_INGESTION_ENABLED='true'
$env:ANALYTICS_SERVICE_MYSQL_DSN='analytics_core:analytics_core@tcp(127.0.0.1:23306)/analytics_core?parseTime=true'
$env:ANALYTICS_SERVICE_MYSQL_AUTO_MIGRATE='true'
$env:ANALYTICS_SERVICE_CLICKHOUSE_ADDR='127.0.0.1:29000'
$env:ANALYTICS_SERVICE_CLICKHOUSE_DATABASE='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_USER='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_PASSWORD='analytics_core'
```

ClickHouse event and property tables remain an explicit schema concern by
default. When ingestion is enabled, startup checks that each enabled source
already has its routed event table and, when property indexing is enabled, the
matching property table.

For local or small deployments, the service can create the routed ClickHouse
tables before that validation step:

```powershell
$env:ANALYTICS_SERVICE_CLICKHOUSE_AUTO_MIGRATE='true'
```

This switch creates the per-source event table and matching `_properties` table
for every enabled source in the current runtime config. Production deployments
should leave it off until schema review, migration ordering, and rollback
procedures are owned by the deployment pipeline. The runtime worker wires Redis
Stream, MySQL checkpoint guards, ClickHouse native batch writers, and typed
property indexing.

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
