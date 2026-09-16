from app import jobs


def test_job_state_is_loaded_from_sqlite(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "DATA_DIR", tmp_path / "data")
    jobs._jobs.clear()
    item = {
        "job_id": "persisted-job",
        "status": "completed",
        "progress": 1,
        "created_at": jobs._now(),
        "updated_at": jobs._now(),
    }

    jobs._persist_job(item)

    assert jobs.get_job("persisted-job")["status"] == "completed"


def test_unfinished_job_is_marked_failed_after_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "DATA_DIR", tmp_path / "data")
    jobs._jobs.clear()
    jobs._persist_job({
        "job_id": "interrupted-job",
        "status": "processing",
        "progress": 0.5,
        "created_at": jobs._now(),
        "updated_at": jobs._now(),
    })

    assert jobs.recover_interrupted_jobs() == 1
    recovered = jobs.get_job("interrupted-job")
    assert recovered["status"] == "failed"
    assert "服务重启" in recovered["error"]


def test_job_summary_counts_persisted_states(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "DATA_DIR", tmp_path / "data")
    jobs._jobs.clear()
    for status in ["completed", "completed", "failed"]:
        job_id = f"{status}-{len(list((tmp_path / 'data').glob('*')))}-{jobs._now()}"
        jobs._persist_job({"job_id": job_id, "status": status, "created_at": jobs._now()})

    summary = jobs.job_summary()

    assert summary["completed"] == 2
    assert summary["failed"] == 1
