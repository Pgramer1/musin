"""Deterministic sampling by hashed id (FR-12)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Resolution of the hash bucket: fractions are honoured to 1e-6.
BUCKETS = 1_000_000


def sample_by_hash(df: DataFrame, column: str, fraction: float | None) -> DataFrame:
    """Keep rows whose hashed `column` falls in the first `fraction` of buckets.

    Input: any frame with `column`. Output: the same columns, a deterministic
    subset (same ids on every run and machine). `fraction=None` returns `df`.
    """
    if fraction is None:
        return df
    if not 0 < fraction <= 1:
        raise ValueError(f"sample fraction must be in (0, 1], got {fraction}")
    return df.filter(F.pmod(F.xxhash64(F.col(column)), F.lit(BUCKETS)) < int(fraction * BUCKETS))
