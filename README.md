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
- Serve internal `GET /v1/realtime`, `GET /v1/events`, and `GET /v1/properties` readback when query mode is enabled.
- Resolve write keys to runtime source configuration.
- Override untrusted client-supplied tenant, project, source, and source type.
- Enforce source enabled state and origin allowlists before analytics-core sees the event.
- Apply analytics-core collect stages for bot filtering, internal traffic filtering, client enrichment, and session derivation.
- Optionally run the queue ingestion worker that writes accepted events to ClickHouse through `analytics-core`.

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
    "visit_salt":"local-visit-salt",
    "visit_window_seconds":1800,
    "client_hash_salt":"local-client-salt",
    "include_client_fingerprint":true
  }
]'
go run ./cmd/simpletrack-anaysitics-service
```

`session_salt`, `visit_salt`, and `client_hash_salt` are server-only runtime
secrets. They must come from the control plane or local runtime config and must
not be derived from the public write key shown in a browser snippet.

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

When query mode is enabled, the service also exposes internal readback routes.
Here, readback means trusted server-side queries that read already accepted
analytics events back from storage for Realtime and Events screens; it is not a
browser-facing collection API or event replay:

- `GET /v1/realtime`
- `GET /v1/events`
- `GET /v1/properties`

These routes require an internal bearer token and use `X-SimpleTrack-Write-Key`
or `write_key` to resolve the source boundary before calling
`analytics-core` storage contracts. Realtime and Events use
`storage.EventReader`; property metadata uses `storage.PropertyCatalogReader`
when a MySQL DSN is configured. Production dashboard readback should stay
server-side in `simpletrack-saas` so the internal bearer token never reaches the
browser. `OPTIONS /v1/realtime`, `OPTIONS /v1/events`, and
`OPTIONS /v1/properties` exist for protocol completeness and trusted
service/browser-shell integrations, but the actual GET request still requires
the bearer token, token route scope, optional token write-key allowlist, source allowlist check, and source
`readback_policy`.

```powershell
$env:ANALYTICS_SERVICE_QUERY_ENABLED='true'
$env:ANALYTICS_SERVICE_QUERY_TOKEN='query-service-token'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES='realtime,events,properties'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_WRITE_KEYS='wk_local'
$env:ANALYTICS_SERVICE_CLICKHOUSE_ADDR='127.0.0.1:29000'
$env:ANALYTICS_SERVICE_CLICKHOUSE_DATABASE='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_USER='analytics_core'
$env:ANALYTICS_SERVICE_CLICKHOUSE_PASSWORD='analytics_core'

Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/realtime?write_key=wk_local' `
  -Headers @{ Authorization = "Bearer query-service-token" }

Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/events?write_key=wk_local&from=2026-05-03T00:00:00Z&to=2026-05-04T00:00:00Z' `
  -Headers @{ Authorization = "Bearer query-service-token" }
```

To read the source-scoped property catalog used for future UI filter
suggestions, provide the MySQL DSN as well. When auto migration is disabled,
`property_catalog` must already exist:

```powershell
$env:ANALYTICS_SERVICE_MYSQL_DSN='analytics_core:analytics_core@tcp(127.0.0.1:23306)/analytics_core?parseTime=true'
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/properties?write_key=wk_local&scope=event&limit=100' `
  -Headers @{ Authorization = "Bearer query-service-token" }
```

Events readback also accepts repeatable `property_filter` query parameters. Each
value is URL-encoded JSON, and each selector must appear in the source's
`allowed_property_filters` runtime config before the request reaches
`analytics-core`:

```powershell
$filter = [uri]::EscapeDataString('{"scope":"event","name":"button","type":"string","op":"eq","value":"hero"}')
$planFilter = [uri]::EscapeDataString('{"scope":"user","name":"plan","type":"string","op":"eq","value":"pro"}')
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8080/v1/events?write_key=wk_local&from=2026-05-03T00:00:00Z&to=2026-05-04T00:00:00Z&event_name=cta_click&sort_field=event_name&property_filter=$filter&property_filter=$planFilter" `
  -Headers @{ Authorization = "Bearer query-service-token" }
```

For query token rotation, keep `ANALYTICS_SERVICE_QUERY_TOKEN` as the current
token and temporarily add the previous token to
`ANALYTICS_SERVICE_QUERY_TOKENS_JSON`:

```powershell
$env:ANALYTICS_SERVICE_QUERY_TOKEN='query-service-token-v2'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_ID='current'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_EXPIRES_AT='2026-05-04T12:00:00Z'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES='realtime,events,properties'
$env:ANALYTICS_SERVICE_QUERY_TOKENS_JSON='[
  {"id":"previous","token":"query-service-token-v1","expires_at":"2026-05-04T10:15:00Z","scopes":["events"]}
]'
```

Rotation should be short-lived: deploy the service accepting both tokens,
switch `simpletrack-saas` to the new token, then remove the old token from the
JSON allowlist once all callers have moved. This service only enforces accepted
runtime tokens; token creation, storage, and operator workflow remain owned by
deployment and the SaaS control-plane environment. Legacy string arrays such as
`["query-service-token-v1"]` still work, but structured entries add a bounded
activation/expiry window, optional per-route `scopes`, optional `write_keys`
source allowlists, and emit audit logs when a rotated token is accepted, an
out-of-scope or out-of-source token is presented, or an expired/not-yet-valid
token is presented.

Use `ANALYTICS_SERVICE_QUERY_TOKEN_WRITE_KEYS` for the primary token or
`write_keys` inside `ANALYTICS_SERVICE_QUERY_TOKENS_JSON` when one token should
be limited to a subset of runtime sources. This lets deployments keep separate
readback tokens per route family and per source without changing the control
plane or browser-visible URLs.

By default the service only accepts `/collect` and durably enqueues events to Redis for local convenience. Production deployments should use Kafka as the primary EventBus provider:

```powershell
$env:ANALYTICS_SERVICE_EVENTBUS='kafka'
$env:ANALYTICS_SERVICE_KAFKA_BROKERS='127.0.0.1:29092'
$env:ANALYTICS_SERVICE_KAFKA_TOPIC='analytics.events'
$env:ANALYTICS_SERVICE_KAFKA_DEAD_LETTER_TOPIC='analytics.events.dead'
$env:ANALYTICS_SERVICE_KAFKA_MAX_ATTEMPTS='5'
$env:ANALYTICS_SERVICE_KAFKA_RETRY_BACKOFF='250ms'
$env:ANALYTICS_SERVICE_KAFKA_WORKERS='100'
```

Kafka deployments can expose a default-disabled internal diagnostics route for
operators. It reuses the same bearer query-token lifecycle as readback routes,
but it is process-scoped and does not require `write_key` because it does not
read source data. Use a dedicated `kafka_diagnostics` token scope when possible:

```powershell
$env:ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_ENABLED='true'
$env:ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_PATH='/v1/kafka/diagnostics'
$env:ANALYTICS_SERVICE_QUERY_TOKEN='kafka-diagnostics-token'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES='kafka_diagnostics'

Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8080/v1/kafka/diagnostics' `
  -Headers @{ Authorization = "Bearer kafka-diagnostics-token" }
```

The response includes worker queue pressure, completion-gate pressure, retry and
DLQ counters, pause/resume counters, paused partitions, ordered commit
`next_offset`, and `lag_estimate`. Treat it as a local diagnostic snapshot, not
as authoritative broker lag, billing evidence, or an SLA metric. Broker secrets,
TLS/SASL material, and raw event payloads are intentionally not returned.

For Prometheus-style scraping, enable the separate default-disabled metrics
route. It exports the same sanitized Kafka provider snapshot as counters and
gauges, including retry/DLQ counters, worker queue pressure, completion-gate
pressure, paused partitions, and per topic-partition ordered commit gauges. Use
the `kafka_metrics` scope so dashboards and scrapers do not need the broader
diagnostics permission:

```powershell
$env:ANALYTICS_SERVICE_KAFKA_METRICS_ENABLED='true'
$env:ANALYTICS_SERVICE_KAFKA_METRICS_PATH='/v1/kafka/metrics'
$env:ANALYTICS_SERVICE_QUERY_TOKEN='kafka-metrics-token'
$env:ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES='kafka_metrics'

Invoke-WebRequest -Method Get -Uri 'http://127.0.0.1:8080/v1/kafka/metrics' `
  -Headers @{ Authorization = "Bearer kafka-metrics-token" }
```

`simpletrack_kafka_ordered_commit_lag_estimate` remains process-local, not
authoritative broker lag. Keep broker-side lag, ISR, controller, and topic
health alerts on Kafka exporter or broker metrics.

Redis Stream remains suitable for local, small-volume, and test deployments. To run ingestion in the same process for local or small deployments, opt in:

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
procedures are owned by the deployment pipeline. The runtime worker wires the
configured EventBus, MySQL checkpoint guards, ClickHouse native batch writers,
and typed property indexing. It also records observed event and user property selectors in
the MySQL `property_catalog` table by default. That catalog is metadata
governance for UI filter suggestions and future allowlists; it does not replace
the ClickHouse `_properties` table used for typed property filtering. When
`ANALYTICS_SERVICE_MYSQL_AUTO_MIGRATE=false`, startup now requires the table to
already exist so a missing metadata table fails before queue consumers repeatedly
retry accepted messages. Disable cataloging only for narrow diagnostics:

```powershell
$env:ANALYTICS_SERVICE_PROPERTY_CATALOGING='false'
```

For a throwaway demo without Redis or Kafka, opt into the non-durable in-memory queue:

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
