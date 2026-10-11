"""Provenance helpers: which graph is this, and which code built it (F3.2, F3.3)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fingerprint(df: DataFrame) -> dict:
    """Row count and an order-independent content hash of a frame.

    In: any frame. Out: {"rows": int, "fingerprint": str}: the sum of the
    xxhash64 of every row (all columns), as an exact decimal so the sum cannot
    overflow. Partitioning and row order do not change it; any changed,
    added, or removed row almost surely does.
    """
    row = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)")).alias("hash"),
    ).collect()[0]
    return {"rows": row["rows"], "fingerprint": str(row["hash"] or 0)}


def repo_commit(repo_root: Path) -> str | None:
    """Commit hash of the checkout, read from .git without the git binary (absent in the image)."""
    git = repo_root / ".git"
    try:
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head
        ref = head[5:]
        if (git / ref).is_file():
            return (git / ref).read_text(encoding="utf-8").strip()
        for line in (git / "packed-refs").read_text(encoding="utf-8").splitlines():
            if line.endswith(" " + ref):
                return line.split()[0]
    except OSError:
        return None
    return None
