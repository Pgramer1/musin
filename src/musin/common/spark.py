"""SparkSession factory shared by every pipeline stage."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from musin.common.config import require_env

# Pipeline stages run on every core. The Spark scaling benchmark (F5.1, D-041)
# pins the core count and turns the event log on for its own process with
# use_benchmark_settings(); nothing else changes these.
_master = "local[*]"
_extra_conf: dict[str, str] = {}


def use_benchmark_settings(cores: int, event_log_dir: Path) -> None:
    """Make every later get_spark() in this process use `cores` cores and keep an event log.

    For musin.benchmark.scaling only (F5.1, NFR-04). Each stage stops its own
    session, so the setting has to outlive one SparkSession; it lives for the
    process, which the benchmark starts fresh for every grid cell.
    """
    if cores < 1:
        raise ValueError(f"cores must be >= 1, got {cores}")
    event_log_dir.mkdir(parents=True, exist_ok=True)
    global _master
    _master = f"local[{cores}]"
    _extra_conf.update(
        {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": event_log_dir.as_uri(),
        }
    )


def get_spark(app_name: str) -> SparkSession:
    """Local-mode SparkSession sized from .env (SPARK_DRIVER_MEMORY).

    spark.driver.memory only takes effect if no JVM is running yet, so call
    this before anything else touches Spark. The session time zone is pinned
    to UTC so date handling does not depend on the machine.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(_master)
        .config("spark.driver.memory", require_env("SPARK_DRIVER_MEMORY"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in _extra_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
