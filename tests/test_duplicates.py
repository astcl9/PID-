import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import fingerprint, jobs, main, storage
from app.main import app


CSV = "timestamp,A.PV\n2026-01-01 00:00:00,1\n2026-01-01 00:01:00,2\n"


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    monkeypatch.setattr(main, "DATA_DIR", root)
    monkeypatch.setattr(jobs, "DATA_DIR", root)
    jobs._jobs.clear()
    return root


def _client():
    return TestClient(app)


def _write_meta(root, dataset_id, **extra):
    payload = {"id": dataset_id, "name": f"{dataset_id}.csv", "rows": 2,
               "start_time": "2026-01-01 00:00:00", "end_time": "2026-01-01 00:01:00",
               "signals": [], "storage": "parquet"}
    payload.update(extra)
    meta_dir = root / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / f"{dataset_id}.json").write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _meta(fp, dataset_id="d1"):
    return {"id": dataset_id, "name": fp["filename"], "rows": 2,
            "fingerprint": fp, "source_name": fp["filename"], "source_size": fp["size"]}


# ---------- 单元：fingerprint 模块 ----------

def test_fingerprint_stable_and_matchable(tmp_path):
    path = tmp_path / "a.csv"
    path.write_text(CSV, encoding="utf-8")
    fp = fingerprint.file_fingerprint(path, "a.csv")
    assert fp["size"] == path.stat().st_size
    assert len(fp["head_sha256"]) == 64
    assert fingerprint.find_matches([_meta(fp)], fp)


def test_legacy_meta_matches_by_name_and_size():
    fp = {"filename": "old.csv", "size": 1234, "head_sha256": "x"}
    legacy = {"id": "legacy", "name": "old.csv", "source_size": 1234}
    assert fingerprint.find_matches([legacy], fp)[0]["id"] == "legacy"


def test_different_content_does_not_match():
    fp1 = {"filename": "a.csv", "size": 10, "head_sha256": "h1"}
    fp2 = {"filename": "a.csv", "size": 10, "head_sha256": "h2"}
    assert fingerprint.find_matches([_meta(fp1)], fp2) == []


def test_duplicate_groups_clusters_same_key_only():
    fp = {"filename": "a.csv", "size": 10, "head_sha256": "h"}
    metas = [_meta(fp, "a"), _meta(fp, "b"),
             _meta({"filename": "c.csv", "size": 1, "head_sha256": "z"}, "c")]
    groups = fingerprint.duplicate_groups(metas)
    assert [[m["id"] for m in g] for g in groups] == [["a", "b"]]


# ---------- 集成：上传记录指纹 ----------

def test_upload_records_fingerprint_and_flags_duplicate(workspace):
    client = _client()
    first = client.post("/api/datasets", files={"file": ("dup.csv", CSV, "text/csv")})
    assert first.status_code == 202
    job_id = first.json()["job_id"]
    deadline = time.time() + 60
    while time.time() < deadline:
        job = client.get(f"/api/imports/{job_id}").json()
        if job["status"] == "completed":
            break
        assert job["status"] != "failed", job.get("error")
        time.sleep(0.2)
    metas = storage.list_meta()
    assert len(metas) == 1
    assert metas[0]["fingerprint"]["head_sha256"]
    assert metas[0]["source_name"] == "dup.csv"

    import hashlib
    digest = hashlib.sha256(CSV.encode("utf-8")).hexdigest()
    check = client.post("/api/datasets/check", json={"name": "dup.csv", "size": len(CSV.encode("utf-8")),
                                                      "head_sha256": digest})
    assert check.status_code == 200
    assert check.json()["duplicate"] is True
    assert check.json()["matches"][0]["id"] == metas[0]["id"]


# ---------- 集成：重复分组与删除 ----------

def test_duplicates_endpoint_groups_identical_datasets(workspace):
    fp = {"algo": "sha256-head-1m", "size": 5, "head_sha256": "h", "filename": "x.csv"}
    _write_meta(workspace, "d1", fingerprint=fp, source_name="x.csv", source_size=5)
    _write_meta(workspace, "d2", fingerprint=fp, source_name="x.csv", source_size=5)
    _write_meta(workspace, "d3", name="y.csv", fingerprint={"size": 9, "head_sha256": "z", "filename": "y.csv"})
    groups = _client().get("/api/duplicates").json()
    assert len(groups) == 1
    assert sorted(d["id"] for d in groups[0]["datasets"]) == ["d1", "d2"]


def test_delete_requires_confirm_and_removes_all_storage(workspace):
    _write_meta(workspace, "victim")
    (workspace / "raw" / "victim").mkdir(parents=True)
    (workspace / "raw" / "victim" / "victim.csv").write_text(CSV, encoding="utf-8")
    part = workspace / "parquet" / "victim" / "date=2026-01-01"
    part.mkdir(parents=True)
    (part / "data.parquet").write_bytes(b"parquet")
    (workspace / "segments").mkdir(exist_ok=True)
    (workspace / "segments" / "victim.json").write_text("{}", encoding="utf-8")

    client = _client()
    assert client.delete("/api/datasets/victim").status_code == 400
    response = client.delete("/api/datasets/victim", params={"confirm": "true"})
    assert response.status_code == 200, response.text
    assert response.json()["deleted"] == "victim"
    assert not (workspace / "meta" / "victim.json").exists()
    assert not (workspace / "raw" / "victim").exists()
    assert not (workspace / "parquet" / "victim").exists()
    assert not (workspace / "segments" / "victim.json").exists()
    assert client.delete("/api/datasets/victim", params={"confirm": "true"}).status_code == 404


def test_delete_unknown_dataset_is_404(workspace):
    assert _client().delete("/api/datasets/ghost", params={"confirm": "true"}).status_code == 404
