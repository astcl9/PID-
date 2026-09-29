import math
import os
import pandas as pd
from .storage import load_frame, query_buckets as query_parquet_buckets
from . import db

BUCKETS = {"10s": 10, "1m": 60, "10m": 600, "1h": 3600}
# Upper bound on the number of points returned by one /api/data call. When
# many tags are requested at once (the frontend caps at 50 by default, and
# each bucket emits up to two extrema), the payload would grow to ~100k+
# points; coarsen the bucket until everything fits. Per-tag budget never
# drops below 200 points so a handful of signals keeps full resolution.
MAX_TOTAL_POINTS = int(os.getenv("PID_MAX_TOTAL_POINTS", "60000"))


def choose_resolution(start, end, requested="auto") -> str:
    if requested != "auto": return requested
    span = pd.Timestamp(end) - pd.Timestamp(start)
    return "1m" if span <= pd.Timedelta(days=2) else ("10m" if span <= pd.Timedelta(days=14) else "1h")


def _cap_bucket(resolution: str, start_ts, end_ts, tag_count: int) -> str:
    """Coarsen the bucket so tag_count signals fit MAX_TOTAL_POINTS.

    A full day at 1m is 1440 buckets (up to 2880 extrema) per tag; with the
    default 50-tag cap that is ~144k points of JSON. Rounding the bucket up
    to the next supported size keeps single-signal views untouched while
    bounding many-signal payloads. Only ever coarsens, never refines.
    """
    if resolution not in BUCKETS or not tag_count:
        return resolution
    order = ["10s", "1m", "10m", "1h"]
    budget_per_tag = max(200, MAX_TOTAL_POINTS // max(1, int(tag_count)))
    span_seconds = max(1.0, (end_ts - start_ts).total_seconds())
    idx = order.index(resolution)
    while idx < len(order) - 1 and span_seconds / BUCKETS[order[idx]] > budget_per_tag:
        idx += 1
    return order[idx]

def _iso_milliseconds(ts: pd.Series) -> pd.Series:
    """Vectorised replacement for the per-row isoformat lambda.

    Naive timestamps render as ``YYYY-MM-DDTHH:MM:SS.mmm``; aware timestamps
    keep their numeric offset, with ``+00:00`` normalised to ``Z`` exactly like
    the old ``isoformat().replace("+00:00", "Z")`` chain. pandas strftime is
    Cython-backed, so this is ~10x faster than a Python lambda per row.
    """
    base = ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3]
    if getattr(ts.dt, "tz", None) is None:
        return base
    off = ts.dt.strftime("%z")  # +0000 / +0800 (no colon, portable on Windows)
    off = off.str[:-2] + ":" + off.str[-2:]
    return base + off.str.replace("+00:00", "Z", regex=False)


def query_frame(dataset_id, start, end, tags=None, resolution="auto", max_points=5000):
    """Query bucketed extrema and return ``(resolution, DataFrame)``.

    The DataFrame carries datetime64 timestamps and ``tag/type/value``
    columns; callers that need ISO strings or per-row dicts format them
    themselves. This avoids the previous double conversion (records ->
    DataFrame) on the hot /api/data path.
    """
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    if start_ts >= end_ts or max_points < 1:
        return resolution, pd.DataFrame(columns=["timestamp", "tag", "type", "value"])
    resolution = choose_resolution(start, end, resolution)
    span = end_ts - start_ts
    if resolution == "raw" and span > pd.Timedelta(hours=6):
        resolution = "10s" if span <= pd.Timedelta(days=2) else "1m"
    resolution = _cap_bucket(resolution, start_ts, end_ts, len(tags) if tags else 0)
    if db.uses_database(dataset_id) and resolution != "raw":
        frame = db.query_buckets(dataset_id, start_ts, end_ts, tags=tags, seconds=BUCKETS[resolution])
    elif resolution != "raw":
        frame = query_parquet_buckets(dataset_id, start_ts, end_ts, tags=tags, seconds=BUCKETS[resolution])
    else:
        frame = load_frame(dataset_id, start_ts, end_ts, tags=tags)
        frame = frame[["timestamp", "tag", "type", "value"]] if not frame.empty else frame
    if frame.empty:
        return resolution, pd.DataFrame(columns=["timestamp", "tag", "type", "value"])
    frame = frame.copy()
    # Downsample each signal independently, retaining bucket extrema.
    if len(frame) > max_points * max(1, frame["tag"].nunique()):
        out = []
        for _, part in frame.groupby(["tag", "type"], sort=False):
            if len(part) <= max_points: out.append(part)
            else:
                idx = pd.Index(range(0, len(part), max(1, math.ceil(len(part) / max_points))))
                out.append(part.iloc[idx[:max_points]])
        frame = pd.concat(out, ignore_index=True)
    return resolution, frame


def query_data(dataset_id, start, end, tags=None, resolution="auto", max_points=5000):
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    if start_ts >= end_ts or max_points < 1:
        return {"resolution": resolution, "rows": []}
    resolution, frame = query_frame(dataset_id, start, end, tags=tags, resolution=resolution, max_points=max_points)
    if frame.empty: return {"resolution": resolution, "rows": []}
    frame["timestamp"] = _iso_milliseconds(pd.to_datetime(frame.timestamp))
    return {"resolution": resolution, "rows": frame.where(pd.notna(frame), None).to_dict("records")}

def daily_summary(dataset_id):
    if db.uses_database(dataset_id):
        return db.daily_summary(dataset_id)
    df = load_frame(dataset_id)
    if df.empty: return []
    df["day"] = pd.to_datetime(df.timestamp).dt.date.astype(str)
    return df.groupby(["day", "tag", "type"], as_index=False).agg(count=("value", "count"), mean=("value", "mean"), min=("value", "min"), max=("value", "max")).to_dict("records")
