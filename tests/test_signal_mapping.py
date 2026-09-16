import json

import pandas as pd
from fastapi.testclient import TestClient

from app import main, storage


def _metadata(root):
    path = root / "meta" / "dataset-map.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "id": "dataset-map",
        "name": "mapping.csv",
        "rows": 1,
        "signals": [{"tag": "TEMP.VALUE", "type": "other"}],
    }), encoding="utf-8")


def test_signal_mapping_is_persisted_and_applied(tmp_path, monkeypatch):
    root = tmp_path / "data"
    _metadata(root)
    monkeypatch.setattr(storage, "DATA_DIR", root)

    response = TestClient(main.app).patch("/api/datasets/dataset-map/signals", json={
        "signals": [{"tag": "TEMP.VALUE", "type": "PV", "unit": "°C", "note": "主温度"}]
    })

    assert response.status_code == 200
    assert response.json()[0] == {"tag": "TEMP.VALUE", "type": "PV", "unit": "°C", "note": "主温度"}
    frame = main._apply_signal_types(pd.DataFrame({"tag": ["TEMP.VALUE"], "type": ["other"]}), "dataset-map")
    assert frame["type"].tolist() == ["PV"]


def test_signal_mapping_rejects_unknown_tag(tmp_path, monkeypatch):
    root = tmp_path / "data"
    _metadata(root)
    monkeypatch.setattr(storage, "DATA_DIR", root)

    response = TestClient(main.app).patch("/api/datasets/dataset-map/signals", json={
        "signals": [{"tag": "UNKNOWN", "type": "MV"}]
    })

    assert response.status_code == 400
    assert "未知信号" in response.json()["detail"]
