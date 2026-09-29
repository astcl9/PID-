from pathlib import Path
import csv
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import io
import json
import logging
import os
import shutil
import pandas as pd
from pydantic import BaseModel
from .config import DATA_DIR, MAX_UPLOAD_MB
from .jobs import create_job, get_job, job_summary, pick_backend
from uuid import uuid4
from . import fingerprint as content_fp
from .storage import list_meta, load_frame, update_signal_metadata, csv_stream, cache_day_summaries
from .query import query_data, query_frame, _iso_milliseconds
from .segmentation import segment_dataframe
from . import segment_store
from .models import (
    DataResponse,
    DatasetInfo,
    DayInfo,
    HealthResponse,
    ImportAccepted,
    Segment,
    SignalMappingRequest,
    SignalInfo,
    StorageHealthResponse,
    SystemHealthResponse,
)

app = FastAPI(title="PID Data Visualization API", version="0.1.0")
logger = logging.getLogger(__name__)
SEGMENT_CACHE_MAX = int(os.getenv("PID_SEGMENT_CACHE_MAX", "32"))
SEGMENTS: dict[str, list[dict]] = {}
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")


def _cache_get(key: str) -> list[dict] | None:
    """Return a cached segmentation, moving it to the most-recently-used end."""
    value = SEGMENTS.get(key)
    if value is not None:
        del SEGMENTS[key]
        SEGMENTS[key] = value
    return value


def _cache_put(key: str, value: list[dict]) -> None:
    """Store a segmentation, evicting least-recently-used entries over budget.

    The on-disk per-dataset store remains the source of truth; this in-memory
    cache only avoids re-reading JSON, so eviction never loses data.
    """
    if key in SEGMENTS:
        del SEGMENTS[key]
    SEGMENTS[key] = value
    while len(SEGMENTS) > SEGMENT_CACHE_MAX:
        del SEGMENTS[next(iter(SEGMENTS))]


def _segments_for_frame(frame: pd.DataFrame, dataset_id: str, loop: str | None = None) -> list[dict]:
    """Segment each control loop independently to avoid cross-loop mixing."""
    if frame.empty:
        return []
    if loop:
        frame = frame[frame["tag"].astype(str).str.startswith(f"{loop}.")]
        result = segment_dataframe(frame, dataset_id=dataset_id) if not frame.empty else []
        for item in result:
            item["loop"] = loop
        return result
    tags = frame["tag"].astype(str)
    loops = sorted({tag.rsplit(".", 1)[0] for tag in tags if "." in tag})
    if not loops:
        return segment_dataframe(frame, dataset_id=dataset_id)
    result: list[dict] = []
    for name in loops:
        part = frame[tags.str.startswith(f"{name}.")]
        items = segment_dataframe(part, dataset_id=dataset_id) if not part.empty else []
        for item in items:
            item["loop"] = name
        result.extend(items)
    return sorted(result, key=lambda item: (str(item.get("start", "")), str(item.get("id", ""))))


def _validate_window(start: str, end: str) -> tuple[str, str]:
    try:
        start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    except (TypeError, ValueError, OverflowError) as exc:
        raise HTTPException(400, "时间格式无效，请使用 ISO 日期时间") from exc
    # pd.Timestamp("") parses to NaT instead of raising; treat any NaT
    # (unparseable or empty component) as an invalid window, not a valid one.
    if pd.isna(start_ts) or pd.isna(end_ts):
        raise HTTPException(400, "时间格式无效，请使用 ISO 日期时间")
    if start_ts >= end_ts:
        raise HTTPException(400, "结束时间必须晚于开始时间")
    return start, end


def _signal_type_map(dataset_id: str) -> dict[str, str]:
    metadata = next((item for item in list_meta() if item.get("id") == dataset_id), None)
    return {str(item.get("tag")): str(item.get("type", "other")) for item in (metadata or {}).get("signals", [])}


def _apply_signal_types(frame: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    if frame.empty or "tag" not in frame or "type" not in frame:
        return frame
    mapping = _signal_type_map(dataset_id)
    if mapping:
        frame = frame.copy()
        frame["type"] = frame["tag"].astype(str).map(mapping).fillna(frame["type"])
    return frame

@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")

@app.get("/health", response_model=HealthResponse)
def health(): return {"status": "ok"}

@app.get("/health/storage", response_model=StorageHealthResponse)
def storage_health():
    from . import db
    configured = os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).lower()
    backend = "postgres" if configured in {"postgres", "postgresql"} else ("clickhouse" if db.enabled() else "parquet")
    return {"backend": backend, "ok": db.ping() if db.enabled() else True}


@app.get("/health/system", response_model=SystemHealthResponse)
def system_health():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(DATA_DIR)
    free_percent = (usage.free / usage.total * 100) if usage.total else 0.0
    return {
        "status": "ok" if free_percent >= 5 else "degraded",
        "disk": {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "free_percent": round(free_percent, 2),
        },
        "jobs": job_summary(),
    }

@app.post("/api/datasets", response_model=ImportAccepted, status_code=202)
async def upload_dataset(file: UploadFile = File(...)):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}: raise HTTPException(400, "仅支持CSV/XLSX")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    length = file.headers.get("content-length")
    if length and int(length) > max_bytes: raise HTTPException(413, "文件超过大小限制")
    staging = DATA_DIR / "staging"; staging.mkdir(parents=True, exist_ok=True)
    dataset_id = uuid4().hex
    temp = staging / f"{dataset_id}{suffix}"
    size = 0
    with temp.open("wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk: break
            size += len(chunk)
            if size > max_bytes:
                temp.unlink(missing_ok=True)
                raise HTTPException(413, "文件超过大小限制")
            out.write(chunk)
    backend = pick_backend(temp, file.filename or temp.name)
    fingerprint = content_fp.file_fingerprint(temp, file.filename or temp.name)
    job_id = create_job(temp, dataset_id, file.filename or temp.name, backend=backend, fingerprint=fingerprint)
    logger.info("accepted upload filename=%s dataset_id=%s job_id=%s bytes=%s", file.filename, dataset_id, job_id, size)
    return JSONResponse(status_code=202, content={"job_id": job_id, "status": "queued"})

@app.get("/api/imports/{job_id}")
def import_status(job_id: str):
    job = get_job(job_id)
    if not job: raise HTTPException(404, "导入任务不存在")
    return job

@app.get("/api/datasets", response_model=list[DatasetInfo])
def datasets():
    # Keep the picker response small for datasets with thousands of signals.
    # Full signal metadata is loaded only after a dataset is selected.
    result = []
    for item in list_meta():
        signals = item.get("signals") or []
        result.append({
            key: item.get(key)
            for key in ("id", "name", "start_time", "end_time", "rows", "warnings", "loops")
            if key in item
        } | {"signal_count": len(signals), "signals": []})
    return result


class DatasetCheckRequest(BaseModel):
    name: str
    size: int
    head_sha256: str


def _metas_with_legacy_sizes() -> list[dict]:
    """Datasets imported before fingerprinting have no source_size in their
    metadata; recover it from the preserved raw copy so name+size matching
    still flags them as duplicates."""
    metas = list_meta()
    for meta in metas:
        if meta.get("fingerprint") or meta.get("source_size") is not None:
            continue
        raw_dir = DATA_DIR / "raw" / str(meta.get("id"))
        try:
            files = list(raw_dir.glob("*"))
        except OSError:
            files = []
        if files:
            meta["source_size"] = files[0].stat().st_size
    return metas


@app.post("/api/datasets/check")
def check_dataset(payload: DatasetCheckRequest):
    """Pre-upload duplicate check: same content fingerprint or, for datasets
    imported before fingerprinting, same original filename + size."""
    matches = content_fp.find_matches(_metas_with_legacy_sizes(), {"filename": payload.name, "size": payload.size,
                                                                    "head_sha256": payload.head_sha256})
    return {"duplicate": bool(matches),
            "matches": [{"id": m.get("id"), "name": m.get("name"), "rows": m.get("rows"),
                         "start_time": m.get("start_time"), "end_time": m.get("end_time")} for m in matches]}


@app.get("/api/duplicates")
def duplicates():
    """Groups of datasets that look identical (same fingerprint or, for legacy
    imports, same original filename + size).  Frontend offers per-group cleanup."""
    groups = content_fp.duplicate_groups(_metas_with_legacy_sizes())
    return [{"key": str(index),
             "datasets": [{"id": m.get("id"), "name": m.get("name"), "rows": m.get("rows"),
                           "start_time": m.get("start_time"), "end_time": m.get("end_time"),
                           "storage": m.get("storage")} for m in group]}
            for index, group in enumerate(groups)]


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: str, confirm: bool = False):
    """Delete a dataset and all of its storage (raw copy, day partitions,
    segment cache, database rows).  Destructive: requires confirm=true."""
    if not confirm:
        raise HTTPException(400, "删除操作不可逆，请带 confirm=true 明确确认")
    metadata = next((item for item in list_meta() if item.get("id") == dataset_id), None)
    if metadata is None:
        raise HTTPException(404, "数据集不存在")
    from . import db
    backend = db.dataset_backend(dataset_id)
    if backend == "clickhouse":
        raise HTTPException(400, "暂不支持删除 ClickHouse 数据集，请手动清理")
    deleted_rows = 0
    if db.dataset_uses_postgres(dataset_id):
        from . import postgres_db
        deleted_rows = postgres_db.delete_dataset(dataset_id)
    for path in (DATA_DIR / "meta" / f"{dataset_id}.json",
                 DATA_DIR / "segments" / f"{dataset_id}.json"):
        path.unlink(missing_ok=True)
    segment_store.delete_dataset(DATA_DIR / "segments", dataset_id)
    for directory in (DATA_DIR / "raw" / dataset_id, DATA_DIR / "parquet" / dataset_id):
        if directory.exists():
            shutil.rmtree(directory, ignore_errors=True)
    for key in [key for key in list(SEGMENTS) if key == dataset_id or key.startswith(f"{dataset_id}:")]:
        del SEGMENTS[key]
    db.clear_backend(dataset_id)
    logger.info("deleted dataset_id=%s backend=%s db_rows=%s", dataset_id, backend, deleted_rows)
    return {"deleted": dataset_id, "backend": backend, "db_rows": deleted_rows}

@app.get("/api/datasets/{dataset_id}/signals", response_model=list[SignalInfo])
def signals(dataset_id: str):
    for item in list_meta():
        if item.get("id") == dataset_id: return item.get("signals", [])
    raise HTTPException(404, "数据集不存在")


@app.patch("/api/datasets/{dataset_id}/signals", response_model=list[SignalInfo])
def update_signals(dataset_id: str, payload: SignalMappingRequest):
    try:
        updated = update_signal_metadata(dataset_id, [item.model_dump() for item in payload.signals])
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if updated is None:
        raise HTTPException(404, "数据集不存在")
    logger.info("updated signal mappings dataset_id=%s count=%s", dataset_id, len(payload.signals))
    return updated

@app.get("/api/datasets/{dataset_id}/days", response_model=list[DayInfo])
def days(dataset_id: str):
    metadata = next((item for item in list_meta() if item.get("id") == dataset_id), None)
    cached_days = (metadata.get("day_summaries") or metadata.get("days")) if metadata else None
    if cached_days:
        # Import jobs publish day partitions only after completion. Returning the
        # cached calendar avoids scanning a full year merely to paint the grid.
        if isinstance(cached_days[0], str):
            # Legacy imports only stored date strings. Treat that as an
            # uncomputed summary so the annual view does not permanently show
            # zero segments after the first request.
            metadata = dict(metadata)
            metadata["segment_summaries_ready"] = False
            cached_days = None
        # Older imports published day summaries before segmentation was run and
        # therefore stored segmentCount=0 for every day. Upgrade that cache on
        # the first calendar request so the annual view agrees with /api/segments.
        if metadata.get("segment_summaries_ready") is not True:
            df = load_frame(dataset_id)
            if df.empty:
                return []
            result = []
            df["day"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")
            for day, part in df.groupby("day"):
                part = _apply_signal_types(part, dataset_id)
                timestamps = pd.to_datetime(part["timestamp"]).drop_duplicates().sort_values()
                expected = max(1, int((timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds() / 60) + 1) if len(timestamps) else 1
                coverage = min(1.0, len(timestamps) / expected)
                segs = _segments_for_frame(part.drop(columns=["day"]), dataset_id=dataset_id)
                result.append({"date": day, "coverage": coverage, "segmentCount": len(segs), "alarmCount": 0,
                               "score": round(max(0.0, 100.0 - float(part["value"].dropna().std() or 0)), 2)})
            cache_day_summaries(dataset_id, result)
            return result
        return cached_days
    df = load_frame(dataset_id)
    if df.empty: return []
    result = []
    df["day"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")
    for day, part in df.groupby("day"):
        part = _apply_signal_types(part, dataset_id)
        timestamps = pd.to_datetime(part["timestamp"]).drop_duplicates().sort_values()
        expected = max(1, int((timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds() / 60) + 1) if len(timestamps) else 1
        coverage = min(1.0, len(timestamps) / expected)
        segs = _segments_for_frame(part.drop(columns=["day"]), dataset_id=dataset_id)
        result.append({"date": day, "coverage": coverage, "segmentCount": len(segs), "alarmCount": 0,
                       "score": round(max(0.0, 100.0 - float(part["value"].dropna().std() or 0)), 2)})
    # The fallback runs a full-dataset scan plus per-day segmentation; persist
    # it so only the first request pays that cost (see storage.cache_day_summaries).
    cache_day_summaries(dataset_id, result)
    return result

@app.get("/api/data", response_model=DataResponse)
def data(dataset_id: str, start: str, end: str, tags: str | None = None, resolution: str = "auto", loop: str | None = None):
    start, end = _validate_window(start, end)
    tag_list = tags.split(",") if tags else None
    actual_resolution, frame = query_frame(dataset_id, start, end, tags=tag_list, resolution=resolution)
    if frame.empty:
        return {"resolution": actual_resolution, "series": []}
    type_mapping = _signal_type_map(dataset_id)
    if type_mapping:
        frame["type"] = frame["tag"].astype(str).map(type_mapping).fillna(frame["type"])
    if loop:
        frame = frame[frame["tag"].astype(str).str.startswith(f"{loop}.")]
    frame = frame.copy()
    frame["timestamp"] = _iso_milliseconds(pd.to_datetime(frame["timestamp"]))
    series = [
        {"name": name, "type": str(part["type"].iloc[0]).upper(),
         "x": part["timestamp"].tolist(),
         "y": part["value"].where(pd.notna(part["value"]), None).tolist()}
        for name, part in frame.groupby("tag", sort=False)
    ]
    return {"resolution": actual_resolution, "series": series}

@app.get("/api/segments", response_model=list[Segment])
def segments(dataset_id: str, start: str | None = None, end: str | None = None, date: str | None = None,
             loop: str | None = None, recompute: bool = False):
    if date:
        # Date queries use a strict half-open interval and include the whole
        # final minute without duplicating midnight in the following day.
        try:
            start, end = f"{date}T00:00:00", f"{datePlus(date)}T00:00:00"
        except (TypeError, ValueError, OverflowError) as exc:
            raise HTTPException(400, "日期格式无效，请使用 YYYY-MM-DD") from exc
    if not start or not end: raise HTTPException(400, "需要 start/end 或 date")
    start, end = _validate_window(start, end)
    key = f"{dataset_id}:{start}:{end}:{loop or ''}"
    result = None if recompute else _cache_get(key)
    if result is None:
        store_dir = DATA_DIR / "segments"
        stored = None if recompute else segment_store.get(store_dir, dataset_id, key)
        if stored is not None:
            result = stored
            _cache_put(key, result)
        else:
            # Segmentation only needs 1-minute resolution, so query bucketed
            # extrema instead of loading every raw sample.  This keeps memory
            # bounded for large single-day datasets (both Parquet and PostgreSQL
            # backends already aggregate inside the database engine).
            bounded = query_data(dataset_id, start, end, resolution="1m", max_points=100000)
            frame = pd.DataFrame(bounded.get("rows", []))
            if not frame.empty and "timestamp" in frame:
                frame["timestamp"] = pd.to_datetime(frame["timestamp"])
            frame = _apply_signal_types(frame, dataset_id)
            result = _segments_for_frame(frame, dataset_id=dataset_id, loop=loop)
            _cache_put(key, result)
            segment_store.put(store_dir, dataset_id, key, result)
    logger.info("served segments dataset_id=%s loop=%s start=%s end=%s count=%s", dataset_id, loop, start, end, len(result))
    return result


@app.get("/api/segments/export")
def export_segments(dataset_id: str, start: str | None = None, end: str | None = None,
                    date: str | None = None, loop: str | None = None):
    values = segments(dataset_id=dataset_id, start=start, end=end, date=date, loop=loop)
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", "loop", "start", "end", "type", "confidence", "reason", "note", "stats"])
    for item in values:
        writer.writerow([
            item.get("id"), item.get("loop"), item.get("start"), item.get("end"),
            item.get("type"), item.get("confidence"), item.get("reason"), item.get("note"),
            json.dumps(item.get("stats", {}), ensure_ascii=False, default=str),
        ])
    label = date or str(start or "segments")[:10]
    return StreamingResponse(iter([output.getvalue().encode("utf-8-sig")]), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="segments-{label}.csv"'})

@app.patch("/api/segments/{segment_id}")
def update_segment(segment_id: str, type: str | None = None, start: str | None = None,
                   end: str | None = None, note: str | None = None):
    if type is None and start is None and end is None and note is None:
        raise HTTPException(400, "没有提供任何修改字段")
    # Manual corrections are persisted per dataset, so a PATCH must find the
    # segment on disk rather than only in the in-memory SEGMENTS cache.  This
    # makes edits survive a service restart. The SQLite store indexes rows by
    # segment id, so this is a single indexed lookup instead of scanning every
    # dataset's store.
    store_dir = DATA_DIR / "segments"
    found = segment_store.find(store_dir, segment_id)
    if found is None:
        raise HTTPException(404, "分段不存在")
    dataset_id, key, item = found
    if type: item["type"] = type
    if start: item["start"] = start
    if end: item["end"] = end
    if note is not None: item["note"] = note
    item["manual_override"] = True
    segment_store.save_item(store_dir, dataset_id, key, item)
    # Keep the in-memory cache in sync when it was already populated.
    _cache_put(key, segment_store.items_for_key(store_dir, dataset_id, key))
    return item

@app.get("/api/export")
def export_data(dataset_id: str, date: str, loop: str | None = None, start: str | None = None, end: str | None = None):
    try:
        start = start or f"{date}T00:00:00"
        end = end or f"{datePlus(date)}T00:00:00"
    except (TypeError, ValueError, OverflowError) as exc:
        raise HTTPException(400, "日期格式无效，请使用 YYYY-MM-DD") from exc
    start, end = _validate_window(start, end)
    from . import db
    type_map = _signal_type_map(dataset_id)
    tags = [tag for tag in type_map if tag.startswith(f"{loop}.")] if loop else None
    if db.uses_database(dataset_id):
        return StreamingResponse(db.csv_stream(dataset_id, start, end, tags=tags), media_type="text/csv",
                                 headers={"Content-Disposition": f'attachment; filename="pid-{date}.csv"'})
    return StreamingResponse(csv_stream(dataset_id, start, end, tags=tags, type_map=type_map), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="pid-{date}.csv"'})

def datePlus(date: str) -> str:
    value = pd.Timestamp(date) + pd.Timedelta(days=1)
    return value.strftime("%Y-%m-%d")
