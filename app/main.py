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
from .config import DATA_DIR, MAX_UPLOAD_MB
from .jobs import create_job, get_job, job_summary, pick_backend
from uuid import uuid4
from .storage import list_meta, load_frame, update_signal_metadata, csv_stream
from .query import query_data
from .segmentation import segment_dataframe
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


def _segment_store_path(dataset_id: str) -> Path:
    path = DATA_DIR / "segments"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{dataset_id}.json"


def _read_segment_store(dataset_id: str) -> dict[str, list[dict]]:
    path = _segment_store_path(dataset_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (ValueError, OSError, TypeError):
        return {}


def _write_segment_store(dataset_id: str, store: dict[str, list[dict]]) -> None:
    path = _segment_store_path(dataset_id)
    path.write_text(json.dumps(store, default=str, ensure_ascii=False), encoding="utf-8")


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
    job_id = create_job(temp, dataset_id, file.filename or temp.name, backend=backend)
    logger.info("accepted upload filename=%s dataset_id=%s job_id=%s bytes=%s", file.filename, dataset_id, job_id, size)
    return JSONResponse(status_code=202, content={"job_id": job_id, "status": "queued"})

@app.get("/api/imports/{job_id}")
def import_status(job_id: str):
    job = get_job(job_id)
    if not job: raise HTTPException(404, "导入任务不存在")
    return job

@app.get("/api/datasets", response_model=list[DatasetInfo])
def datasets(): return list_meta()

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
            return [{"date": day, "coverage": 1.0, "segmentCount": 0, "alarmCount": 0, "score": 0} for day in cached_days]
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
        segs = segment_dataframe(part.drop(columns=["day"]), dataset_id=dataset_id)
        result.append({"date": day, "coverage": coverage, "segmentCount": len(segs), "alarmCount": 0,
                       "score": round(max(0.0, 100.0 - float(part["value"].dropna().std() or 0)), 2)})
    return result

@app.get("/api/data", response_model=DataResponse)
def data(dataset_id: str, start: str, end: str, tags: str | None = None, resolution: str = "auto", loop: str | None = None):
    start, end = _validate_window(start, end)
    payload = query_data(dataset_id, start, end, tags.split(",") if tags else None, resolution)
    rows = payload.get("rows", [])
    type_mapping = _signal_type_map(dataset_id)
    for row in rows:
        row["type"] = type_mapping.get(str(row.get("tag")), row.get("type", "other"))
    if loop:
        rows = [r for r in rows if str(r.get("tag", "")).startswith(f"{loop}.")]
    grouped = {}
    for row in rows:
        key = row.get("tag")
        grouped.setdefault(key, {"name": key, "type": row.get("type", "other").upper(), "x": [], "y": []})
        grouped[key]["x"].append(row.get("timestamp")); grouped[key]["y"].append(row.get("value"))
    return {"resolution": payload.get("resolution", resolution), "series": list(grouped.values())}

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
        stored = _read_segment_store(dataset_id)
        if not recompute and key in stored:
            result = stored[key]
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
            stored[key] = result
            _write_segment_store(dataset_id, stored)
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
    # makes edits survive a service restart.
    segments_dir = DATA_DIR / "segments"
    if not segments_dir.exists():
        raise HTTPException(404, "分段不存在")
    for store_path in sorted(segments_dir.glob("*.json")):
        dataset_id = store_path.stem
        stored = _read_segment_store(dataset_id)
        for key, items in stored.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if item.get("id") != segment_id:
                    continue
                if type: item["type"] = type
                if start: item["start"] = start
                if end: item["end"] = end
                if note is not None: item["note"] = note
                item["manual_override"] = True
                _write_segment_store(dataset_id, stored)
                # Keep the in-memory cache in sync when it was already populated.
                _cache_put(key, items)
                return item
    raise HTTPException(404, "分段不存在")

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

