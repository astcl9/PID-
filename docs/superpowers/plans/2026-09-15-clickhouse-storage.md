# ClickHouse Storage Migration Plan

**Goal:** Make ClickHouse the primary store for 200GB+ PID history while keeping the existing Parquet path as a development fallback.

**Architecture:** The API and import worker stream normalized rows into ClickHouse. A MergeTree table is partitioned by dataset and calendar month and ordered by dataset, tag and timestamp; materialized daily/one-minute summaries serve the annual calendar and time-window chart. Metadata and job state remain in the application filesystem initially.

**Tech Stack:** ClickHouse Server 24+, clickhouse-connect, FastAPI, existing pandas/DuckDB fallback.

## Constraints

- Huge imports must use bounded batches; never call `read_csv()` without chunksize for the database path.
- Raw samples use `DateTime64(3)` and local plant time is normalized before insert.
- Queries always include dataset and half-open timestamp filters; chart responses remain bounded per signal.
- ClickHouse is enabled with `PID_STORAGE=clickhouse`; `PID_STORAGE=parquet` keeps local development working.
- XLSX is accepted only as a bounded source; 200GB-scale files must be CSV or Parquet.

### Task 1: Database schema and client

- Add `app/db.py` with lazy client creation, schema bootstrap, batch Parquet insert, bounded query, daily summary query and health check.
- Create `pid_samples` MergeTree table partitioned by `(dataset_id, toYYYYMM(timestamp))`, ordered by `(dataset_id, tag, timestamp)`.
- Create `pid_day_summary` aggregated table or explicit daily-summary inserts.

### Task 2: Storage selection and importer integration

- Route completed import partitions to ClickHouse in 50,000-row Arrow batches when enabled; publish metadata only after the final partition succeeds.
- Keep Parquet writes for fallback mode and existing tests.
- Expose row counts and insert progress in import jobs.

### Task 3: Query/API integration

- Make `load_frame`, `query_data`, daily days, segments and exports use ClickHouse in primary mode.
- Push time/tag filters and aggregation into SQL; only materialize the requested window.
- Add `/health/storage` and clear error responses when ClickHouse is unavailable.

### Task 4: Deployment and verification

- Add ClickHouse service and persistent volume to Docker Compose.
- Add `clickhouse-connect` dependency and environment variables.
- Verify schema bootstrap, 100k-row streamed insert, one-hour query, day summary, restart persistence, and Parquet fallback.
