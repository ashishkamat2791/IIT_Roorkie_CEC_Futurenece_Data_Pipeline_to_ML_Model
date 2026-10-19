"""
Extraction: read a source CSV exactly as text, enforce the column contract and
stage it as a run-scoped work file for the next task.

Data never travels through XCom; only small metadata dicts do.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from .config import ORDER_COLUMNS, SOURCES, RunConfig

log = logging.getLogger(__name__)


class SchemaChangeError(ValueError):
    """Raised when a source file's header no longer matches the agreed contract."""


def read_header(path: Path) -> list[str]:
    with open(path, newline="") as f:
        return next(csv.reader(f), [])


def check_schema(path: Path, expected: list[str]) -> None:
    header = read_header(path)
    if header != expected:
        missing = [c for c in expected if c not in header]
        extra = [c for c in header if c not in expected]
        raise SchemaChangeError(
            f"{path.name}: schema contract broken. missing={missing} unexpected={extra} "
            f"header={header}. Refusing to load: a renamed or dropped column would load wrong data silently."
        )


def read_text_csv(path: Path) -> pd.DataFrame:
    """Every value as a string; blanks stay '' and whitespace is preserved."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    df.insert(0, "_row_num", range(1, len(df) + 1))
    df["_source_file"] = path.name
    return df


def file_md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def work_path(cfg: RunConfig, name: str) -> Path:
    p = cfg.run_work_dir / f"{name}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def write_work(df: pd.DataFrame, path: Path) -> str:
    df.to_csv(path, index=False)
    return str(path)


def read_work(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    if "_row_num" in df.columns:
        df["_row_num"] = df["_row_num"].astype(int)
    return df


def extract_source(cfg: RunConfig, source: str) -> dict:
    spec = SOURCES[source]
    path = cfg.data_dir / spec["file"]
    if not path.exists():
        raise FileNotFoundError(f"required source file missing: {path}")
    check_schema(path, spec["columns"])
    df = read_text_csv(path)
    if df.empty:
        raise ValueError(f"{path.name} has a header but no rows")
    out = write_work(df, work_path(cfg, f"extract_{source}"))
    meta = {
        "source": source, "file": spec["file"], "rows": len(df), "work_file": out,
        "md5": file_md5(path), "extracted_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
        "run_id": cfg.run_id,
    }
    log.info("[extract] %s rows=%s md5=%s", spec["file"], len(df), meta["md5"])
    return meta


def extract_increment(cfg: RunConfig) -> dict:
    if not cfg.apply_increment:
        log.info("[extract] apply_increment=False: no incremental order file this run")
        return {"source": "orders_increment", "file": cfg.increment_file, "rows": 0,
                "work_file": None, "applied": False}
    path = cfg.data_dir / cfg.increment_file
    if not path.exists():
        raise FileNotFoundError(f"apply_increment=True but {path} does not exist")
    check_schema(path, ORDER_COLUMNS)
    df = read_text_csv(path)
    out = write_work(df, work_path(cfg, "extract_orders_increment"))
    log.info("[extract] %s rows=%s", path.name, len(df))
    return {"source": "orders_increment", "file": path.name, "rows": len(df), "work_file": out,
            "applied": True, "md5": file_md5(path), "run_id": cfg.run_id}
