from datetime import timedelta
import uuid
import numpy as np
import pandas as pd

DEFAULTS = {"min_segment_minutes": 5, "gap_minutes": 2, "setpoint_step": None,
            "slope_z": 3.0, "steady_std": 0.5, "saturation_rate": 0.5,
            "sustained_error": 2.0}


def _series_stats(series: pd.Series) -> dict:
    values = series.dropna()
    if values.empty:
        return {"mean": None, "min": None, "max": None, "std": None, "first": None,
                "last": None, "count": 0, "missing_rate": 1.0}
    std = values.std()
    return {
        "mean": float(values.mean()),
        "min": float(values.min()),
        "max": float(values.max()),
        "std": float(std) if pd.notna(std) else 0.0,
        "first": float(values.iloc[0]),
        "last": float(values.iloc[-1]),
        "count": int(values.count()),
        "missing_rate": float(series.isna().mean()),
    }


def _segment_stats(part: pd.DataFrame, start, end, segment_type: str) -> dict:
    err = (part.PV - part.SV).dropna()
    mv_values = part.MV.dropna()
    saturation_rate = float(((mv_values <= 1.0) | (mv_values >= 99.0)).mean()) if not mv_values.empty else None
    signed_error = err.mean() if not err.empty else None
    absolute_error = err.abs().mean() if not err.empty else None
    duration = int((end - start).total_seconds() // 60)
    return {
        "duration_minutes": duration,
        "rows": int(len(part)),
        "pv": _series_stats(part.PV),
        "sv": _series_stats(part.SV),
        "mv": _series_stats(part.MV),
        "pv_sv_mean_error": float(signed_error) if pd.notna(signed_error) else None,
        "pv_sv_mean_absolute_error": float(absolute_error) if pd.notna(absolute_error) else None,
        "mv_saturation_rate": saturation_rate,
        "steady_minutes": duration if segment_type == "steady" else 0,
    }

def segment_dataframe(df: pd.DataFrame, config=None, dataset_id="") -> list[dict]:
    cfg = {**DEFAULTS, **(config or {})}
    if df.empty: return []
    piv = df.pivot_table(index="timestamp", columns="type", values="value", aggfunc="mean").resample("1min").mean()
    if piv.empty: return []
    for c in ["PV", "SV", "MV"]:
        if c not in piv: piv[c] = np.nan
    pv, sp, mv = piv.PV, piv.SV, piv.MV
    med = pv.diff().abs().median() or 1.0
    sp_step = sp.diff().abs() > (cfg["setpoint_step"] or max(1.0, 5 * (sp.diff().abs().median() or 0)))
    pv_jump = pv.diff().abs() > max(3 * med, 1e-9)
    mv_jump = mv.diff().abs() > max(3 * (mv.diff().abs().median() or 1), 1e-9)
    gap = pv.isna() & pv.groupby(pv.notna().cumsum()).transform("size").ge(int(cfg["gap_minutes"]))
    boundaries = sorted(set([piv.index[0], *piv.index[sp_step | pv_jump | mv_jump | gap], piv.index[-1] + timedelta(minutes=1)]))
    segs = []
    for a, b in zip(boundaries, boundaries[1:]):
        part = piv[(piv.index >= a) & (piv.index < b)]
        if part.empty: continue
        err = (part.PV - part.SV).dropna()
        pv_std = part.PV.std()
        std = float(pv_std) if pd.notna(pv_std) else 0.0
        typ, reason, conf = "unknown", "特征不足", .4
        saturation = ((part.MV.dropna() <= 1.0) | (part.MV.dropna() >= 99.0)).mean() if part.MV.notna().any() else 0.0
        mean_abs_error = float(err.abs().mean()) if not err.empty else 0.0
        if part.PV.isna().mean() >= .5: typ, reason, conf = "data_gap", "PV数据缺失", .98
        elif bool(sp_step.loc[a:b].any()): typ, reason, conf = "setpoint_change", "检测到设定值阶跃", .93
        elif bool(mv_jump.loc[a:b].any() or pv_jump.loc[a:b].any()): typ, reason, conf = "transition", "PV/MV快速变化", .85
        elif saturation >= cfg["saturation_rate"]: typ, reason, conf = "saturated", "MV长时间接近输出上下限", .9
        elif len(err) >= 4 and mean_abs_error >= cfg["sustained_error"]: typ, reason, conf = "deviation", "PV-SV偏差持续偏大", .82
        elif len(err) >= 4 and np.std(err) > max(1, 2 * np.median(np.abs(err - np.median(err)))): typ, reason, conf = "oscillation", "PV-SV误差波动较大", .78
        elif std <= cfg["steady_std"]: typ, reason, conf = "steady", "PV波动较小", .88
        stats = _segment_stats(part, a, b, typ)
        segs.append({"id": uuid.uuid4().hex, "dataset_id": dataset_id, "start": a.to_pydatetime(), "end": b.to_pydatetime(), "type": typ, "confidence": float(conf), "reason": reason, "stats": stats})
    # Coalesce noise fragments while preserving explicit data gaps.
    minimum = timedelta(minutes=int(cfg["min_segment_minutes"]))
    merged = []
    for seg in segs:
        if merged and seg["type"] != "data_gap" and (seg["end"] - seg["start"]) < minimum:
            prev = merged[-1]
            prev["end"] = seg["end"]
            prev["reason"] = f"{prev['reason']}；{seg['reason']}"
            prev["confidence"] = max(prev["confidence"], seg["confidence"])
        else:
            merged.append(seg)
    for seg in merged:
        part = piv[(piv.index >= seg["start"]) & (piv.index < seg["end"])]
        seg["stats"] = _segment_stats(part, seg["start"], seg["end"], seg["type"])
    return merged
