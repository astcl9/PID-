"""Content fingerprints for duplicate-import detection.

The fingerprint is the file size plus the SHA-256 of the first HEAD_BYTES of
the file.  The browser computes the same value client-side before uploading so
the server can warn about an already-imported file without transferring it
first.  Keep HEAD_BYTES in sync with the frontend (app/static/app.js).
"""
from __future__ import annotations

import hashlib
from pathlib import Path

HEAD_BYTES = 1024 * 1024


def head_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        digest.update(handle.read(HEAD_BYTES))
    return digest.hexdigest()


def file_fingerprint(path: Path, filename: str | None = None) -> dict:
    path = Path(path)
    return {
        "algo": "sha256-head-1m",
        "size": path.stat().st_size,
        "head_sha256": head_sha256(path),
        "filename": Path(filename or path.name).name,
    }


def fingerprint_key(fp: dict | None) -> tuple | None:
    """Normalised comparison key for a fingerprint dict, or None if unusable."""
    if not isinstance(fp, dict):
        return None
    size, digest = fp.get("size"), fp.get("head_sha256")
    if size is None or not digest:
        return None
    return (int(size), str(digest))


def meta_key(meta: dict) -> tuple | None:
    """Comparison key for an imported dataset's recorded metadata."""
    return fingerprint_key(meta.get("fingerprint"))


def legacy_key(meta: dict) -> tuple | None:
    """Fallback key for datasets imported before fingerprints existed."""
    name = meta.get("source_name") or meta.get("name")
    size = meta.get("source_size")
    if not name or size is None:
        return None
    return (str(name), int(size))


def find_matches(metas: list[dict], fp: dict) -> list[dict]:
    """Datasets that look identical to the file behind fingerprint ``fp``."""
    wanted_fp = fingerprint_key(fp)
    wanted_legacy = (str(fp.get("filename") or ""), int(fp.get("size") or -1))
    matches = []
    for meta in metas:
        if wanted_fp and meta_key(meta) == wanted_fp:
            matches.append(meta)
            continue
        # Datasets imported before fingerprint recording: same original file
        # name and size is a strong enough candidate to warrant a warning.
        if not meta.get("fingerprint") and legacy_key(meta) == wanted_legacy:
            matches.append(meta)
    return matches


def duplicate_groups(metas: list[dict]) -> list[list[dict]]:
    """Cluster datasets that share a fingerprint or a legacy name+size key."""
    buckets: dict[tuple, list[dict]] = {}
    for meta in metas:
        key = meta_key(meta) or legacy_key(meta)
        if key is None:
            continue
        buckets.setdefault(key, []).append(meta)
    return [sorted(group, key=lambda m: str(m.get("id"))) for group in buckets.values() if len(group) > 1]
