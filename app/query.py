import math
import pandas as pd
from .storage import load_frame, query_buckets as query_parquet_buckets
from . import db

def choose_resolution(start, end, requested="auto") -> str:
    if requested != "auto": return requested
    span = pd.Timestamp(end) - pd.Timestamp(start)
    return "1m" if span <= pd.Timedelta(days=2) else ("10m" if span <= pd.Timedelta(days=14) else "1h")

def query_data(dataset_id, start, end, tags=None, resolution="auto", max_points=5000):
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    if start_ts >= end_ts or max_points < 1:
        return {"resolution": resolution, "rows": []}
    resolution = choose_resolution(start, end, resolution)
    span = end_ts - start_ts
    if resolution == "raw" and span > pd.Timedelta(hours=6):
        resolution = "10s" if span <= pd.Timedelta(days=2) else "1m"
    if db.uses_database(dataset_id) and resolution != "raw":
        seconds = {"10s": 10, "1m": 60, "10m": 600, "1h": 3600}[resolution]
        frame = db.query_buckets(dataset_id, start_ts, end_ts, tags=tags, seconds=seconds)
        if frame.empty:
            return {"resolution": resolution, "rows": []}
        frame["timestamp"] = pd.to_datetime(frame.timestamp).map(lambda x: x.isoformat(timespec="milliseconds").replace("+00:00", "Z") if getattr(x, "tzinfo", None) else x.isoformat(timespec="milliseconds"))
        return {"resolution": resolution, "rows": frame.to_dict("records")}
    if resolution != "raw":
        seconds = {"10s": 10, "1m": 60, "10m": 600, "1h": 3600}[resolution]
        df = query_parquet_buckets(dataset_id, start_ts, end_ts, tags=tags, seconds=seconds)
    else:
        df = load_frame(dataset_id, start_ts, end_ts, tags=tags)
    if df.empty: return {"resolution": resolution, "rows": []}
    # Downsample each signal independently, retaining bucket extrema.
    if len(df) > max_points * max(1, df.tag.nunique()):
        out = []
        for _, part in df.groupby(["tag", "type"], sort=False):
            if len(part) <= max_points: out.append(part)
            else:
                idx = pd.Index(range(0, len(part), max(1, math.ceil(len(part) / max_points))))
                out.append(part.iloc[idx[:max_points]])
        df = pd.concat(out, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df.timestamp).map(lambda x: x.isoformat(timespec="milliseconds").replace("+00:00", "Z") if getattr(x, "tzinfo", None) else x.isoformat(timespec="milliseconds"))
    return {"resolution": resolution, "rows": df.where(pd.notna(df), None).to_dict("records")}

def daily_summary(dataset_id):
    if db.uses_database(dataset_id):
        return db.daily_summary(dataset_id)
    df = load_frame(dataset_id)
    if df.empty: return []
    df["day"] = pd.to_datetime(df.timestamp).dt.date.astype(str)
    return df.groupby(["day", "tag", "type"], as_index=False).agg(count=("value", "count"), mean=("value", "mean"), min=("value", "min"), max=("value", "max")).to_dict("records")
