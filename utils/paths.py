# utils/paths.py
from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict


def obj_slug(object_name: str) -> str:
    # Simple, safe slug for folder/file names
    return object_name.strip().lower()


def build_paths(object_name: str, base_dir: str = "data") -> Dict[str, str]:
    """
    Canonical file paths for a given object.
    - raw:       data/raw/<object>.csv
    - processed: data/processed/<object>/summary.csv
    - output:    data/output/<object>/summary.json
    """
    slug = obj_slug(object_name)
    raw_csv = Path(base_dir) / "raw" / f"{slug}.csv"
    processed_dir = Path(base_dir) / "processed" / slug
    processed_csv = processed_dir / "summary.csv"
    output_dir = Path(base_dir) / "output" / slug
    out_json = output_dir / "summary.json"

    processed_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_csv.parent.mkdir(parents=True, exist_ok=True)

    return {
        "raw_csv": str(raw_csv),
        "processed_csv": str(processed_csv),
        "out_json": str(out_json),
    }


def build_qc_paths(object_name: str, base_dir: str = "data") -> Dict[str, str]:
    slug = obj_slug(object_name)
    qa_dir = Path(base_dir) / "processed" / "qa" / slug
    qa_dir.mkdir(parents=True, exist_ok=True)

    return {
        "qa_dir": str(qa_dir),
        "dedup_csv": str(qa_dir / "dedup.csv"),
        "profile_json": str(qa_dir / "profile.json"),
        "schema_report_json": str(qa_dir / "schema_report.json"),
        "parquet_snapshot": str(qa_dir / "snapshot.parquet"),
        "rowcount_file": str(qa_dir / "rowcount.txt"),
    }


# ---------------- Meta-store normalization (built-in) ---------------- #

def _merge_jsonl(dst: Path, src: Path) -> None:
    """Append all lines from src into dst, then remove src (and empty parent dirs)."""
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("a", encoding="utf-8") as out, src.open("r", encoding="utf-8") as inp:
        for line in inp:
            if line.strip():
                out.write(line if line.endswith("\n") else line + "\n")
    # cleanup
    try:
        src.unlink(missing_ok=True)
        if src.parent.name == "runs":  # handle legacy meta/<obj>/runs/jsonl
            src.parent.rmdir()
    except Exception:
        pass


def _dedupe_jsonl_inplace(path: Path) -> None:
    """
    De-duplicate JSONL by run_id (keep the last occurrence).
    If run_id is empty/missing, keep the original ordering (treated as unique).
    """
    if not path.exists():
        return
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        return

    by_id: "OrderedDict[tuple[str, int], str]" = OrderedDict()
    for idx, ln in enumerate(lines):
        rid = ""
        try:
            rid = (json.loads(ln).get("run_id") or "").strip()
        except Exception:
            rid = ""
        key = (rid, idx if not rid else 0)  # empty ids remain unique by index; non-empty dedupe by rid
        by_id[key] = ln  # later lines override for same rid

    path.write_text("\n".join(by_id.values()) + "\n", encoding="utf-8")


def normalize_meta_store(object_name: str, base_dir: str = "data") -> Dict[str, str]:
    """
    Normalize meta storage for an object:
      - returns canonical meta paths
      - merges legacy files into canonical ones
      - de-duplicates canonical JSONL
    Call this before reading/writing metadata.
    """
    slug = obj_slug(object_name)
    meta_dir = Path(base_dir) / "meta" / slug
    meta_dir.mkdir(parents=True, exist_ok=True)

    global_meta_dir = Path(base_dir) / "meta"
    global_meta_dir.mkdir(parents=True, exist_ok=True)

    canonical_runs = meta_dir / "runs.jsonl"
    canonical_global_runs = global_meta_dir / "runs.jsonl"

    # Merge legacy per-object
    legacy1 = meta_dir / "runs" / "jsonl"   # folder/file typo
    legacy2 = meta_dir / "runs.josnl"       # ext typo
    if legacy1.exists():
        _merge_jsonl(canonical_runs, legacy1)
    if legacy2.exists():
        _merge_jsonl(canonical_runs, legacy2)

    # Merge legacy global
    legacy_global = global_meta_dir / "runs.josnl"
    if legacy_global.exists():
        _merge_jsonl(canonical_global_runs, legacy_global)

    # De-dup canonical files
    _dedupe_jsonl_inplace(canonical_runs)
    _dedupe_jsonl_inplace(canonical_global_runs)

    # Canonical task logs (reserved)
    tasks_jsonl = meta_dir / "tasks.jsonl"
    global_tasks_jsonl = global_meta_dir / "tasks.jsonl"

    return {
        "meta_dir": str(meta_dir),
        "runs_jsonl": str(canonical_runs),
        "global_runs_jsonl": str(canonical_global_runs),
        "tasks_jsonl": str(tasks_jsonl),
        "global_tasks_jsonl": str(global_tasks_jsonl),
    }


def build_meta_paths(object_name: str, base_dir: str = "data") -> Dict[str, str]:
    """
    Public helper: returns canonical meta paths (after normalization).
    Safe to call from tasks — guarantees clean paths.
    """
    return normalize_meta_store(object_name, base_dir=base_dir)
