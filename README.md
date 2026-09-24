# observer

[![codecov](https://codecov.io/gh/sebastiencs/observer/graph/badge.svg)](https://codecov.io/gh/sebastiencs/observer)

## Local logs storage

`observerd` keeps the acknowledged OTLP payload in the per-tenant WAL, then projects logs into an Arrow memtable and publishes frozen generations as local Parquet. The data directory layout, dynamic column names, JSON fallback, and the durability boundary are documented on `observer-storage`.

A scan sees a generation in memory or in Parquet. The WAL checkpoint advances only after that generation's commit descriptor is durable, and retention removes only sealed segments behind the checkpoint.

`observer-query` runs SQL over one tenant store. The caller chooses the tenant, pins one snapshot, and reads an Arrow stream from a single `logs` table. Results have no order unless the SQL contains `ORDER BY`. `observerd` serves that stream as one buffered JSON document on `POST /v1/query`.

One engine shares a memory pool, a spill directory, a Parquet metadata cache, and a limit on how many queries run at once. Each query has one deadline for planning and reading. A query that cannot start before that deadline is busy. Dropping or cancelling the stream stops the query. Published files and in-memory batches are ordered by event time, then WAL sequence, both descending, so a newest-event `ORDER BY ... LIMIT` with no other predicate can skip older hours and files. An event-time bound can skip hours. Commit statistics can skip Parquet files, and DataFusion can skip row groups and pages. Filters still run on the rows that remain. The stream reports how long the query waited, what it skipped, how many rows it returned, and whether it timed out or was cancelled.

## Query API

Queries use `listen.query`. Ingest and admin listeners do not serve them. `POST /v1/query` reads the bearer token, and the tenant comes only from that token. The body is JSON:

```json
{ "sql": "SELECT body FROM logs", "timeout_ms": 5000, "max_rows": 100 }
```

`sql` is one `SELECT`. `timeout_ms` and `max_rows` are optional and can only lower the server caps. Unknown fields are rejected. The default deadline is 30 seconds and the default row cap is 10,000. Request bodies are limited to 64 KiB. The response is buffered and limited to 16 MiB. A query finishes before the response is written. A failure is `{"error":{"code":"..."}}` and contains no result rows.

```bash
curl -sS -X POST "http://127.0.0.1:8081/v1/query" \
  -H "Authorization: Bearer secret-a" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT body, event_time_unix_nano FROM logs ORDER BY event_time_unix_nano DESC LIMIT 20"}'
```

```bash
curl -sS -X POST "http://127.0.0.1:8081/v1/query" \
  -H "Authorization: Bearer secret-a" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT body FROM logs","timeout_ms":5000,"max_rows":100}'
```

`listen.query` is required. `[query]` is optional. Omitted fields keep the defaults below. Spill files are written under `{data_directory}/query-spill` and are removed when the engine shuts down.

```toml
[listen]
grpc = "0.0.0.0:4317"
http = "0.0.0.0:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"

[query]
timeout_ms = 30000
max_rows = 10000
max_request_bytes = 65536
max_response_bytes = 16777216
memory_pool_bytes = 268435456
max_concurrent_queries = 4
target_partitions_per_query = 4
batch_size = 8192
metadata_cache_bytes = 33554432
spill_budget_bytes = 1073741824
sort_spill_reservation_bytes = 10485760
```

`max_concurrent_queries` and `target_partitions_per_query` default to the process parallelism. The other numbers above are the built-in defaults.

A success response is `application/json`:

```json
{
  "schema": [{ "name": "event_time_unix_nano", "type": "UInt64", "nullable": false }],
  "rows": [{ "event_time_unix_nano": "1700000000000000000" }],
  "metrics": {
    "admission_wait_ns": "0",
    "planning_ns": "0",
    "execution_ns": "0",
    "files_scanned": "0",
    "files_pruned": "0",
    "row_groups_pruned": "0",
    "rows_returned": "1",
    "memory_peak_bytes": "0",
    "spill_bytes": "0",
    "timed_out": false,
    "cancelled": false
  }
}
```

Each schema entry has `name`, `type`, and `nullable`. `type` is a compact Arrow name such as `Utf8`, `Int32`, `Timestamp(Nanosecond, None)`, `List<nullable Utf8>`, `Map<Utf8, Int64>`, `Decimal128(10, 2)`, or `FixedSizeBinary(16)`. `i64`, `u64`, decimals, and temporal values are decimal strings, so a nanosecond timestamp keeps every digit. Intervals are comma-separated decimal strings. Binary and fixed-size binary values are standard base64. Smaller integers and finite floats are JSON numbers. Arrow nulls and non-finite floats are JSON null. Lists, structs, and maps stay nested. A map is an array of `{ "key", "value" }` objects so the key type and order survive.

Every `metrics` counter and duration is a decimal string. Durations are nanoseconds. `memory_peak_bytes` is the shared pool's high-water mark since the engine started. `timed_out` and `cancelled` are booleans.

| Status | `error.code` |
| --- | --- |
| 401 | `unauthenticated`, with `WWW-Authenticate: Bearer` |
| 400 | `invalid_request`, `request_too_large`, or `invalid_sql` |
| 408 | `timeout` |
| 429 | `busy` |
| 422 | `row_limit`, `response_too_large`, or `unsupported_result_type` |
| 503 | `storage` or `resources` |
| 500 | `execution` |

`unsupported_result_type` also includes `data_type`, the short Arrow name that could not be encoded. Error bodies omit filesystem paths, SQL text, and token values.

Any browser origin can call the route. Responses carry `Access-Control-Allow-Origin: *`. Preflight allows `POST` and `OPTIONS`, and the `Authorization` and `Content-Type` headers. Credentials are not allowed.

Deferred: Arrow and NDJSON responses, async jobs, a cancel endpoint, schema discovery, HTTP compression, query gRPC, and distributed queriers.

## Testing

`cargo test` runs the workspace. The HTTP conformance tests start a real `observerd` and use `POST /v1/logs` and `POST /v1/query`.

`logs_http_roundtrip` checks accepted ingestion, core columns, dynamic attributes, active and published storage, SQL filters and aggregates, shutdown of the active tail, and a two-tenant burst. `logs_http_errors` checks that rejected posts never become queryable. `query_http` checks authentication, limits, CORS, and queries that run while logs are ingested. Each case asserts the JSON schema and rows for the logs it sent.

Those process tests do not enumerate every Arrow result type or every generated input. JSON encoding stays covered by the `observerd` unit tests. Projection, dynamic columns, and canonical JSON stay covered by the `observer-storage` unit and property tests. Snapshot isolation under concurrent ingest and publish stays covered by `observer-query`.

## Git hooks

Pre-commit runs `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`. Install once per clone:

```bash
./scripts/install-githooks.sh
```