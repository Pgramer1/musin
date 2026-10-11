"""Recording first release year (F2.4, FR-07).

    python -m musin.clean.years --config config/pipeline.yaml [--sample 0.05]

Reads:  $MUSIN_DATA/<paths.raw>/{recording,track,medium,release_country,
        release_unknown_country}  (F1.3)
Writes: <clean>/recording_years/  (recording_id, first_year), one row per
        recording; <clean> as in D-019 (sample tree with --sample).

Recordings carry no date. A recording's first release year is the earliest
year among all releases it appears on (track -> medium -> release), taking each
release's dated events in `release_country` and `release_unknown_country`. A
release date counts only if its year lies in [years.min_release_year,
snapshot year] (D-023); a recording with no counted date gets first_year null.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.common.config import Config, load_config
from musin.common.layers import layer_path
from musin.common.sampling import sample_by_hash
from musin.common.spark import get_spark

log = logging.getLogger("musin.clean.years")


def release_dates(release_country: DataFrame, release_unknown_country: DataFrame) -> DataFrame:
    """Every dated release event.

    In: release_country(release, date_year), release_unknown_country(release, date_year).
    Out: (release, date_year) for events with a known year; one row per event.
    """
    return (
        release_country.select("release", "date_year")
        .unionByName(release_unknown_country.select("release", "date_year"))
        .filter(F.col("date_year").isNotNull())
    )


def valid_year(min_year: int | None, max_year: int | None):
    """Column predicate on `date_year` for the plausible range; None = unbounded."""
    cond = F.lit(True)
    if min_year is not None:
        cond = cond & (F.col("date_year") >= min_year)
    if max_year is not None:
        cond = cond & (F.col("date_year") <= max_year)
    return cond


def release_years(
    release_country: DataFrame,
    release_unknown_country: DataFrame,
    min_year: int | None,
    max_year: int | None,
) -> DataFrame:
    """Earliest counted year per release.

    In: release_country(release, date_year), release_unknown_country(release, date_year),
    year bounds (inclusive; None = unbounded). Out: (release, year int);
    releases without a counted year are absent.
    """
    return (
        release_dates(release_country, release_unknown_country)
        .filter(valid_year(min_year, max_year))
        .groupBy("release")
        .agg(F.min("date_year").cast("int").alias("year"))
    )


def recording_first_years(track: DataFrame, medium: DataFrame, rel_years: DataFrame) -> DataFrame:
    """First release year per recording.

    In: track(recording, medium), medium(id, release), release_years output.
    Out: (recording, first_year); recordings with no dated release are absent.
    """
    return (
        track.select("recording", F.col("medium").alias("medium_id"))
        .join(medium.select(F.col("id").alias("medium_id"), "release"), "medium_id")
        .join(rel_years, "release")
        .groupBy("recording")
        .agg(F.min("year").alias("first_year"))
    )


def run(cfg: Config, sample: float | None) -> dict:
    """Run the stage; return the logged counts."""
    min_year = int(cfg.get("years.min_release_year"))
    max_year = int(str(cfg.get("snapshot_date"))[:4])
    spark = get_spark("musin-clean-years")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    recordings = sample_by_hash(raw("recording"), "id", sample).select(
        F.col("id").alias("recording")
    )
    recordings = recordings.cache()
    track = raw("track").select("recording", "medium").join(recordings, "recording")
    rc, ru = raw("release_country"), raw("release_unknown_country")

    dates = release_dates(rc, ru).cache()
    first = recording_first_years(track, raw("medium"), release_years(rc, ru, min_year, max_year))
    out = layer_path(cfg, "clean", sample) / "recording_years"
    (
        recordings.join(first, "recording", "left")
        .select(F.col("recording").cast("long").alias("recording_id"), "first_year")
        .write.mode("overwrite")
        .parquet(str(out))
    )
    result = spark.read.parquet(str(out))

    unbounded = recording_first_years(track, raw("medium"), release_years(rc, ru, None, None))
    changed = result.join(
        unbounded.select(
            F.col("recording").cast("long").alias("recording_id"), "first_year"
        ).withColumnRenamed("first_year", "raw_year"),
        "recording_id",
    ).filter(F.col("first_year").isNull() | (F.col("first_year") != F.col("raw_year")))
    on_release = track.select("recording").distinct().count()
    stats = result.agg(
        F.count("*").alias("rows"),
        F.count("first_year").alias("dated"),
        F.min("first_year").alias("min"),
        F.max("first_year").alias("max"),
        F.countDistinct("recording_id").alias("ids"),
    ).collect()[0]
    counts = {
        "recordings_in": recordings.count(),
        "recordings_on_a_release": on_release,
        "release_dates_with_year": dates.count(),
        f"release_dates_before_{min_year}_ignored": dates.filter(
            F.col("date_year") < min_year
        ).count(),
        f"release_dates_after_{max_year}_ignored": dates.filter(
            F.col("date_year") > max_year
        ).count(),
        "rows_out": stats["rows"],
        "recordings_with_first_year": stats["dated"],
        "recordings_without_first_year": stats["rows"] - stats["dated"],
        "recordings_year_changed_by_bounds": changed.count(),
        "first_year_min": stats["min"],
        "first_year_max": stats["max"],
    }
    by_decade = {
        r["decade"]: r["count"]
        for r in result.filter(F.col("first_year").isNotNull())
        .groupBy((F.floor(F.col("first_year") / 10) * 10).cast("int").alias("decade"))
        .count()
        .collect()
    }
    log.info("sample fraction: %s -> %s", sample if sample else "full", out)
    for key, value in counts.items():
        log.info("  %-40s %s", key, value)
    log.info("  recordings by decade: %s", dict(sorted(by_decade.items())))

    problems = []
    if stats["ids"] != stats["rows"] or stats["rows"] != counts["recordings_in"]:
        problems.append("rows out != recordings in, or duplicate recording_id")
    if stats["min"] is not None and (stats["min"] < min_year or stats["max"] > max_year):
        problems.append(f"first_year outside [{min_year}, {max_year}]")
    for df in (recordings, dates):
        df.unpersist()
    spark.stop()
    if problems:
        raise RuntimeError("quality checks failed: " + "; ".join(problems))
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--sample", type=float, help="fraction of recordings, e.g. 0.05")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    run(load_config(args.config), args.sample)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
