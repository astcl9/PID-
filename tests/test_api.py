def test_health():
    from app.main import app
    from fastapi.testclient import TestClient
    assert TestClient(app).get("/health").json() == {"status":"ok"}


def test_invalid_query_window_returns_user_error():
    from app.main import app
    from fastapi.testclient import TestClient

    client = TestClient(app)
    invalid = client.get("/api/data", params={"dataset_id": "d", "start": "not-a-date", "end": "2026-01-02"})
    reversed_window = client.get("/api/data", params={"dataset_id": "d", "start": "2026-01-02", "end": "2026-01-01"})

    assert invalid.status_code == 400
    assert "时间格式无效" in invalid.json()["detail"]
    assert reversed_window.status_code == 400
    assert "结束时间" in reversed_window.json()["detail"]


def test_invalid_segment_date_returns_user_error():
    from app.main import app
    from fastapi.testclient import TestClient

    response = TestClient(app).get("/api/segments", params={"dataset_id": "d", "date": "invalid"})

    assert response.status_code == 400
    assert "日期格式无效" in response.json()["detail"]


def test_system_health_reports_disk_and_job_queue():
    from app.main import app
    from fastapi.testclient import TestClient

    response = TestClient(app).get("/health/system")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"ok", "degraded"}
    assert payload["disk"]["total_bytes"] > 0
    assert set(payload["jobs"]) == {"queued", "processing", "completed", "failed"}


def test_segment_export_returns_csv(monkeypatch):
    from app import main
    from fastapi.testclient import TestClient

    monkeypatch.setattr(main, "segments", lambda **kwargs: [{
        "id": "s1", "loop": "A", "start": "2026-01-01T00:00:00",
        "end": "2026-01-01T00:10:00", "type": "steady", "confidence": 0.9,
        "reason": "PV波动较小", "note": "已确认", "stats": {"duration_minutes": 10},
    }])

    response = TestClient(main.app).get("/api/segments/export", params={"dataset_id": "d", "date": "2026-01-01"})

    assert response.status_code == 200
    assert response.content.startswith(b"\xef\xbb\xbf")
    assert "已确认" in response.content.decode("utf-8-sig")
