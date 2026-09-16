# PostgreSQL Import and Query Implementation Plan

**Goal:** Replace the failed DuckDB import path with PostgreSQL and show twelve labelled months in the running calendar.

**Architecture:** Stream bounded CSV/XLSX batches through COPY into temporary tables and merge into indexed PostgreSQL partitions. Publish dataset metadata only after import completion; committed batches stay invisible during failed imports. Time-bounded queries aggregate inside PostgreSQL and stream exports. Retain existing local Parquet datasets for compatibility.

**Tech Stack:** FastAPI, psycopg 3, PostgreSQL 17, pandas, native JavaScript.

**Spec:** User instructions in this session: use PostgreSQL, support large historical files, repair monthly calendar.

## Constraints

- No full-file DataFrame or global DuckDB deduplication on the PostgreSQL path.
- Preserve strict CSV encoding detection and last-row semantics for duplicate samples.
- Preserve existing datasets; default new imports to PostgreSQL, fail clearly if unavailable.
- Do not claim a 200 GB benchmark without running it.

## Task 1: Storage and query adapter

- [ ] Replace `app/db.py` with psycopg connection/schema/COPY import, atomic metadata publication, indexed bounds, adaptive min/max aggregation and streamed CSV export.
- [ ] Route `app/storage.py`, `app/query.py`, `app/main.py`, `app/config.py` to PostgreSQL while allowing existing local Parquet dataset reads.
- [ ] Test duplicates across chunks, missing values, date boundaries, rollback visibility, bounds, adaptive query limits and encoding using a real isolated PostgreSQL database.

## Task 2: Deployment and large-file entry

- [ ] Replace ClickHouse compose service with PostgreSQL and healthcheck; add psycopg dependency and environment examples.
- [ ] Add server-side import CLI so large files can be imported without uploading or duplicating 200 GB through multipart staging. Document upload limits and disk overhead honestly.
- [ ] Install/check local PostgreSQL runtime, start the project service and verify GBK import, date listing, curves and export over HTTP.

## Task 3: Calendar

- [ ] Render twelve month cards with weekday padding, year/month headings and date tooltips.
- [ ] Check JavaScript syntax and calendar day counts including leap years; verify date click preserves navigation.

## Validation

Run `.venv/Scripts/python.exe -m pytest -q -p no:cacheprovider` using a unique workspace temporary directory and `node --check app/static/app.js`. Run API integration with an isolated database and repeat against the running project service with a small explicitly labelled test dataset. Keep test and production data separate.
