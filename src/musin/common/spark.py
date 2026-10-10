"""SparkSession factory shared by every pipeline stage."""

from __future__ import annotations

from pyspark.sql import SparkSession

from musin.common.config import require_env


def get_spark(app_name: str) -> SparkSession:
    """Local-mode SparkSession sized from .env (SPARK_DRIVER_MEMORY).

    spark.driver.memory only takes effect if no JVM is running yet, so call
    this before anything else touches Spark. The session time zone is pinned
    to UTC so date handling does not depend on the machine.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", require_env("SPARK_DRIVER_MEMORY"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
