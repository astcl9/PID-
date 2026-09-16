import pandas as pd
import json
from fastapi.encoders import jsonable_encoder
from app.segmentation import segment_dataframe

def test_segments_detect_setpoint_change():
    t = pd.date_range("2026-01-01", periods=20, freq="min")
    rows = []
    for i, ts in enumerate(t):
        sp = 10 if i < 10 else 20
        rows += [(ts,"A.PV","PV",sp), (ts,"A.SV","SV",sp), (ts,"A.MV","MV",5)]
    got = segment_dataframe(pd.DataFrame(rows, columns=["timestamp","tag","type","value"]), dataset_id="d")
    assert got and any(x["type"] == "setpoint_change" for x in got)


def test_segments_with_missing_values_are_strict_json_safe():
    t = pd.date_range("2026-01-01", periods=12, freq="min")
    rows = []
    for i, ts in enumerate(t):
        pv = None if 4 <= i < 8 else 10.0
        rows += [(ts, "A.PV", "PV", pv), (ts, "A.SV", "SV", 10.0), (ts, "A.MV", "MV", 5.0)]
    encoded = jsonable_encoder(segment_dataframe(pd.DataFrame(rows, columns=["timestamp", "tag", "type", "value"]), dataset_id="d"))
    json.dumps(encoded, allow_nan=False)


def test_segment_statistics_include_analysis_metrics():
    t = pd.date_range("2026-01-01", periods=10, freq="min")
    rows = []
    for i, ts in enumerate(t):
        rows += [(ts, "A.PV", "PV", 10.0 + i / 10), (ts, "A.SV", "SV", 10.0), (ts, "A.MV", "MV", 100.0 if i == 0 else 50.0)]

    segments = segment_dataframe(pd.DataFrame(rows, columns=["timestamp", "tag", "type", "value"]), dataset_id="d")
    stats = segments[0]["stats"]

    assert stats["duration_minutes"] > 0
    assert stats["pv"]["count"] > 0
    assert stats["pv"]["min"] <= stats["pv"]["max"]
    assert stats["pv_sv_mean_absolute_error"] is not None
    assert stats["mv_saturation_rate"] > 0


def test_segments_detect_sustained_mv_saturation():
    t = pd.date_range("2026-01-01", periods=12, freq="min")
    rows = []
    for ts in t:
        rows += [(ts, "A.PV", "PV", 10.0), (ts, "A.SV", "SV", 10.0), (ts, "A.MV", "MV", 100.0)]

    segments = segment_dataframe(pd.DataFrame(rows, columns=["timestamp", "tag", "type", "value"]), dataset_id="d")

    assert any(item["type"] == "saturated" for item in segments)
