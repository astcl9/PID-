from pathlib import Path

import pandas as pd

from app.importer import import_dataset
from app.query import query_data


def test_chunked_import_reports_progress_and_writes_day_partition(tmp_path):
    source = tmp_path / "history.csv"
    pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=12, freq="min"),
        "TIC101.PV": range(12),
        "TIC101.SV": [5] * 12,
        "TIC101.MV": [20] * 12,
    }).to_csv(source, index=False)
    events = []
    metadata = import_dataset(source, "dataset-1", source.name, data_dir=tmp_path / "data", chunk_rows=3,
                              progress=lambda **fields: events.append(fields))
    assert metadata["days"] == ["2026-01-01"]
    assert metadata["day_summaries"] == [{"date": "2026-01-01", "coverage": 1.0, "segmentCount": 0, "alarmCount": 0, "score": 100.0}]
    assert events[-1]["stage"] == "completed"
    assert events[-1]["progress"] == 1.0
    assert (tmp_path / "data" / "parquet" / "dataset-1" / "date=2026-01-01" / "data.parquet").exists()


def test_chunked_import_accepts_gb18030(tmp_path):
    source = tmp_path / "history-gbk.csv"
    lines = ["时间,回路1.PV,回路1.SV,回路1.MV"]
    lines.extend(f"2026-01-01 00:{i:02d},{i},10,20" for i in range(6))
    source.write_bytes(("\n".join(lines) + "\n").encode("gb18030"))
    metadata = import_dataset(source, "dataset-gbk", source.name, data_dir=tmp_path / "data", chunk_rows=2)
    assert metadata["days"] == ["2026-01-01"]
    assert {item["tag"] for item in metadata["signals"]} == {"回路1.PV", "回路1.SV", "回路1.MV"}


def test_chunked_import_deduplicates_across_chunk_boundaries(tmp_path):
    source = tmp_path / "duplicates.csv"
    pd.DataFrame({
        "timestamp": [
            "2026-01-01 00:00:00",
            "2026-01-01 00:01:00",
            "2026-01-01 00:00:00",
            "2026-01-01 00:02:00",
        ],
        "A.PV": [1.0, 2.0, 9.0, 3.0],
    }).to_csv(source, index=False)

    root = tmp_path / "data"
    metadata = import_dataset(source, "dataset-duplicates", source.name, data_dir=root, chunk_rows=2)
    frame = pd.read_parquet(root / "parquet" / "dataset-duplicates" / "date=2026-01-01" / "data.parquet")

    duplicate_time = pd.Timestamp("2026-01-01 00:00:00")
    assert metadata["rows"] == 3
    assert len(frame) == 3
    assert frame.loc[frame["timestamp"] == duplicate_time, "value"].tolist() == [9.0]


def test_wide_import_skips_memory_heavy_global_sort_with_warning(tmp_path, monkeypatch):
    source = tmp_path / "wide.csv"
    frame = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=8, freq="min")})
    for index in range(20):
        frame[f"TAG{index:02d}.PV"] = index
    frame.to_csv(source, index=False)
    monkeypatch.setenv("PID_MAX_DEDUPE_ROWS", "10")

    metadata = import_dataset(source, "dataset-wide", source.name, data_dir=tmp_path / "data", chunk_rows=2)

    assert metadata["rows"] == 160
    assert any("超过全量去重阈值" in warning for warning in metadata["warnings"])
    assert (tmp_path / "data" / "parquet" / "dataset-wide" / "date=2026-01-01" / "data.parquet").exists()
