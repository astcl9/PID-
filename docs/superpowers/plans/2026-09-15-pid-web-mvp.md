# PID Data Visualization Web MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new LAN-capable web application that imports year-long PID history, browses data by day, automatically detects meaningful time segments, and visualizes PV/SV/MV with zooming and segment statistics.

**Architecture:** A FastAPI backend will parse uploaded CSV/XLSX files into normalized Parquet partitions and query them with DuckDB. A small browser UI served by the backend will use Plotly for the first release, keeping the deployment to one Python process while preserving clean API boundaries for a later Vue/ECharts frontend.

**Tech Stack:** Python 3.11+, FastAPI, Uvicorn, Pandas, DuckDB, PyArrow, python-multipart, openpyxl, Plotly.js, vanilla HTML/CSS/JavaScript.

**Spec:** New project requirements from the user conversation.

## Global Constraints

- Support CSV and XLSX uploads with a required timestamp column and arbitrary PID signal columns.
- Automatically classify signal names containing PV, SV/SP, or MV; allow manual mapping when inference is uncertain.
- Store timestamps in ISO format and preserve the original uploaded file.
- Partition data by calendar day and support daily browsing over a one-year dataset.
- Smart segmentation must use configurable thresholds and return segment type, reason, and confidence.
- Limit chart payloads with server-side resampling; never load an entire year of raw points into the browser.
- Bind the server to `0.0.0.0` for LAN access and document the URL and firewall port.

### Task 1: Project scaffold and data contracts

**Files:**
- Create: `pyproject.toml`
- Create: `app/__init__.py`
- Create: `app/models.py`
- Create: `app/config.py`
- Create: `tests/test_models.py`
- Create: `README.md`

- [ ] Define Pydantic models for dataset metadata, signal metadata, segment records, and query responses.
- [ ] Define configuration for `DATA_DIR`, upload size, host, and port using environment variables.
- [ ] Add pytest configuration and a smoke test for model serialization.
- [ ] Document local startup and LAN startup commands.

### Task 2: Import, normalization, and storage

**Files:**
- Create: `app/importer.py`
- Create: `app/storage.py`
- Create: `tests/test_importer.py`

- [ ] Implement CSV/XLSX loading, timestamp-column detection, numeric coercion, duplicate/ordering checks, and signal classification.
- [ ] Normalize rows to `dataset_id, timestamp, tag, type, value, quality, unit`.
- [ ] Preserve the original file under `data/raw/<dataset_id>/` and write Parquet partitions under `data/parquet/<dataset_id>/date=YYYY-MM-DD/`.
- [ ] Return an import report with row count, time range, detected signals, missing rate, and warnings.
- [ ] Test CSV import, XLSX import, PV/MV/SV inference, and malformed timestamps.

### Task 3: Queries, daily summaries, and resampling

**Files:**
- Create: `app/query.py`
- Create: `tests/test_query.py`

- [ ] Implement dataset listing, signal listing, daily summary generation, and time-window queries.
- [ ] Support `raw`, `1m`, `10s`, and automatic interval selection based on requested time span.
- [ ] Implement min/max bucket resampling with a maximum of 5,000 points per signal.
- [ ] Implement half-open segment windows `[start, end)` and statistics: mean, min, max, std, first, last, count, missing rate.
- [ ] Test daily filtering, resampling bounds, and statistics at segment boundaries.

### Task 4: Smart segmentation

**Files:**
- Create: `app/segmentation.py`
- Create: `tests/test_segmentation.py`

- [ ] Resample input to one-minute values and calculate PV slope, MV slope, rolling variance, PV-SV error, and data gaps.
- [ ] Detect boundaries for setpoint steps, rapid MV/PV changes, sustained error, mode changes when a mode column exists, and gaps longer than the configured limit.
- [ ] Merge segments shorter than the configured minimum unless they contain an alarm or data gap.
- [ ] Classify segments as `steady`, `setpoint_change`, `transition`, `oscillation`, `saturated`, `data_gap`, or `unknown`.
- [ ] Return confidence and human-readable reasons; expose thresholds in configuration.
- [ ] Test each boundary type and verify deterministic segment output.

### Task 5: FastAPI endpoints and browser UI

**Files:**
- Create: `app/main.py`
- Create: `app/static/index.html`
- Create: `app/static/app.js`
- Create: `app/static/styles.css`
- Create: `tests/test_api.py`

- [ ] Add endpoints: `POST /api/datasets`, `GET /api/datasets`, `GET /api/datasets/{id}/signals`, `GET /api/datasets/{id}/days`, `GET /api/data`, `GET /api/segments`, and `GET /api/export`.
- [ ] Serve a single-page UI with upload, dataset selection, date picker, signal checkboxes, interval control, and segment list.
- [ ] Render synchronized PV/SV/MV Plotly charts with wheel zoom, drag pan, range slider, reset, unified hover, and segment overlays.
- [ ] Show selected-segment statistics and allow CSV export.
- [ ] Test upload, metadata, data query, and segment endpoints with FastAPI TestClient.

### Task 6: LAN packaging and verification

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Modify: `README.md`
- Create: `scripts/sample_data.py`

- [ ] Add a deterministic sample generator covering several days, a setpoint change, a transition, and a data gap.
- [ ] Add Docker Compose volume mapping for persistent `data/` storage.
- [ ] Configure Uvicorn to listen on `0.0.0.0:8080` and document Windows firewall and LAN access checks.
- [ ] Run unit tests, generate sample data, start the server, verify `/health`, upload the sample, query a day, and verify segment output.

