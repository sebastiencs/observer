# observer

[![codecov](https://codecov.io/gh/sebastiencs/observer/graph/badge.svg)](https://codecov.io/gh/sebastiencs/observer)

## Local logs storage

`observerd` keeps the acknowledged OTLP payload in the per-tenant WAL, then projects logs into an Arrow memtable and publishes frozen generations as local Parquet. The data directory layout, dynamic column names, JSON fallback, and the durability boundary are documented on `observer-storage`.

A scan sees a generation in memory or in Parquet. The WAL checkpoint advances only after that generation's commit descriptor is durable, and retention removes only sealed segments behind the checkpoint.

`observer-query` runs SQL over one tenant store. The caller chooses the tenant, pins one snapshot, and reads an Arrow stream from a single `logs` table. Results have no order unless the SQL contains `ORDER BY`.

## Git hooks

Pre-commit runs `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`. Install once per clone:

```bash
./scripts/install-githooks.sh
```