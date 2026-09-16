# Large-file Import and Day Time Window Implementation Plan

> **For agentic workers:** Execute the bounded ingestion, query and browser tasks in parallel, then integrate and verify the real upload-to-chart workflow.

**Goal:** Import large CSV/XLSX files without whole-file memory copies, report upload and processing progress, and browse a selected day with a movable time window.

**Architecture:** Multipart uploads spool to disk in bounded reads and return a background import job. The single import worker reads CSV chunks or XLSX rows, writes day partitions and cached day summaries; DuckDB filters and reduces the selected time window before Python serializes bounded chart payloads. A day slider controls a 15/30/60/180/360-minute or full-day view and fetches fresh data as it moves.

**Tech Stack:** Existing FastAPI, Python 3.12 virtual environment, DuckDB, PyArrow, pandas, openpyxl and Plotly browser UI.

**Spec:** User requires large-file imports and a progress-bar style day navigator to inspect different periods of an imported day.

## Global Constraints

- Default upload limit is 2048 MiB and can be configured with PID_MAX_UPLOAD_MB.
- Upload progress is measured network bytes; parsing progress reports actual processed rows, with indeterminate progress where a total is unavailable.
- Failed requests and empty dates must display errors or empty states, never substitute synthetic data.
- A date uses local wall-clock boundaries [00:00, next-day 00:00); timestamps with timezone offsets are converted to Asia/Shanghai before date partitioning.
- Every chart query returns at most 5000 points per signal and preserves local extrema when reducing raw data.
- Original uploads stay on disk. Only completed imports are listed as datasets.

### Task 1: Streaming ingestion

**Files:** app/importer.py, app/ingestion.py, tests/test_ingestion.py.

**Interface:** import_dataset(path, dataset_id, filename, data_dir=None, progress=None, chunk_rows=25000) -> metadata; progress receives keyword fields stage, rows_processed, bytes_processed, total_bytes, progress.

- [ ] Read CSV using chunksize and XLSX via read_only iterator; reject malformed/empty input clearly.
- [ ] Vectorize signal conversion per chunk and write date partitions; compact duplicate samples across chunk boundaries with last-row semantics using disk-backed DuckDB.
- [ ] Publish metadata plus cached day summaries only after all partitions finish.
- [ ] Verify forced small chunks, cross-chunk duplicates, missing values, timezone boundaries and XLSX equivalence.

### Task 2: Bounded queries

**Files:** app/storage.py, app/query.py, tests/test_query.py.

**Interface:** query_data(dataset_id,start,end,tags=None,resolution='auto',max_points=5000,loop=None) -> {resolution,rows}; load_frame remains for small callers. daily_summary returns cached day entries.

- [ ] Prune date files before SQL, push signal/time filters into DuckDB and order samples by timestamp.
- [ ] Aggregate into min/max envelopes without loading full raw ranges; retain null gaps and sub-second timestamps.
- [ ] Verify per-signal cap, spike preservation, half-open midnight, invalid ranges and date-file pruning.

### Task 3: Import jobs and HTTP integration

**Files:** app/jobs.py, app/main.py, app/config.py, tests/test_api.py.

- [ ] POST /api/datasets streams to disk and returns 202 with job_id/status; GET /api/imports/{job_id} reports queued/processing/completed/failed.
- [ ] Enforce file size/type and serialize heavy imports through one worker; surface failure and restart interruption.
- [ ] Supply cached days, bounded chart/segment data and streaming CSV export for the current window.
- [ ] Verify background completion, oversize rejection, failure status and actual metadata/data requests.

### Task 4: Browser time navigator

**Files:** app/static/index.html, app/static/app.js, app/static/styles.css, tests/browser/*.

- [ ] Render upload byte progress with XMLHttpRequest and poll background processing rows until completion.
- [ ] Add accessible native day slider, selected interval text, window duration, previous/next, full-day reset and clock input.
- [ ] Debounce drag requests, abort or discard stale responses and requery after Plotly zoom/pan.
- [ ] Preserve accurate calendar dates, reset state on dataset/day changes and export selected window.
- [ ] Check real upload, drag to morning/evening, last-minute inclusion, no-data states and failure visibility in a browser.

### Task 5: Runtime and documentation

**Files:** README.md, pyproject.toml, docker-compose.yml, scripts/*.

- [ ] Run meaningful importer/query/API regressions in .venv Python 3.12.
- [ ] Verify a generated large file end-to-end and report measured size/rows/timing rather than an untested maximum.
- [ ] Bundle the chart dependency locally for LAN use and document upload limits and lack of resume support.
