"""Generate a small deterministic PID history file for local smoke tests."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate(days: int = 3, output: str = "data/sample_pid.csv") -> Path:
    periods = days * 24 * 60
    # Use plant-local wall-clock timestamps so a one-day fixture stays on one
    # calendar date after the importer applies the Asia/Shanghai policy.
    ts = pd.date_range("2026-01-01", periods=periods, freq="min")
    minute = np.arange(periods)
    sv = np.where(minute < max(1, periods // 2), 80.0, 84.0)
    sv = np.where(minute >= 2 * 24 * 60, 78.0, sv)
    mv = 45 + 4 * np.sin(minute / 90) + np.where(minute >= 24 * 60, 8, 0)
    pv = sv + 1.8 * np.sin(minute / 35) + 0.3 * np.cos(minute / 11)
    pv[900:930] = np.nan
    frame = pd.DataFrame({"timestamp": ts, "TIC101.PV": pv, "TIC101.SV": sv, "TIC101.MV": mv})
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--output", default="data/sample_pid.csv")
    args = parser.parse_args()
    print(generate(args.days, args.output))
