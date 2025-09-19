from pathlib import Path
def obj_slug(object_name: str) -> str:
    # Simple, safe slug for folder/file names
    # (keeps __c if present; it's fine in filenames)
    return object_name.strip().lower()
def build_paths(object_name: str, base_dir: str = "data")-> dict:
    """
    Return canonical file paths for a given object.
    - raw:      data/raw/<object>.csv
    - processed:data/processed/<object>/summary.csv
    - output:   data/output/<object>/summary.json
    """

    slug = obj_slug(object_name)
    raw_csv =Path(base_dir) / "raw" / f"{slug}.csv"
    processed_dir = Path(base_dir) / "processed" / slug
    processed_csv = processed_dir / "summary.csv"
    output_dir    = Path(base_dir) / "output" / slug
    out_json      = output_dir / "summary.json"

    processed_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_csv.parent.mkdir(parents=True, exist_ok=True)

    return {
        "raw_csv": str(raw_csv),
        "processed_csv": str(processed_csv),
        "out_json": str(out_json),
    }


def build_qc_paths(object_name: str, base_dir: str ="data")-> dict:
    slug = obj_slug(object_name)
    qa_dir = Path(base_dir) / "processed" / "qa" /slug
    qa_dir.mkdir(parents=True, exist_ok=True)

    return {
        "qa_dir":str(qa_dir),
        "dedup_csv": str(qa_dir / "dedup.csv"),   
        "profile_json": str(qa_dir / "profile.json"),
        "schema_report_json": str(qa_dir / "schema_report.json"),
        "parquet_snapshot": str(qa_dir / "snapshot.parquet"),
        "rowcount_file": str(qa_dir / "rowcount.txt"),
    }