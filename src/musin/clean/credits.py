"""Credit parsing: main and featured artists per recording (F2.2, FR-05).

    python -m musin.clean.credits --config config/pipeline.yaml [--sample 0.05]

Reads:  $MUSIN_DATA/<paths.raw>/{recording,artist_credit_name}  (F1.3)
        $MUSIN_DATA/<paths.clean>/special_artists/                (F2.1, always full)
Writes: <clean>/credits/  (recording_id, artist_id, position, role)
        where <clean> is $MUSIN_DATA/<paths.clean> in full mode and
        $MUSIN_DATA/sample/f<fraction>/<paths.clean> with --sample (D-019).

PRD §8.2: within a credit, in `position` order, an artist is *featured* if a
join phrase before it matches `credits.featuring_pattern`; otherwise *main*.
Roles are decided on the complete credit and only then are special-purpose
artists removed (D-020), so "[unknown] feat. X" leaves X featured, not main.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from musin.common.config import Config, load_config
from musin.common.layers import layer_path
from musin.common.sampling import sample_by_hash
from musin.common.spark import get_spark

log = logging.getLogger("musin.clean.credits")

MAIN, FEATURED = "main", "featured"


def credit_roles(acn: DataFrame, pattern: str) -> DataFrame:
    """Role of every artist slot in every credit.

    In: artist_credit_name(artist_credit, position, artist, join_phrase).
    Out: (artist_credit, position, artist, role) with role 'featured' when any
    earlier position's join phrase matches `pattern`, else 'main'. One row per
    input row; nothing is removed.
    """
    before = (
        Window.partitionBy("artist_credit")
        .orderBy("position")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    matched = F.max(F.coalesce(F.col("join_phrase"), F.lit("")).rlike(pattern).cast("int"))
    return acn.select(
        "artist_credit",
        "position",
        "artist",
        F.when(matched.over(before) == 1, F.lit(FEATURED)).otherwise(F.lit(MAIN)).alias("role"),
    )


def drop_special(roles: DataFrame, special: DataFrame) -> DataFrame:
    """Remove special-purpose artists after roles are decided.

    In: credit_roles output, special(artist_id). Out: same columns, without
    rows whose artist is special. Other artists keep their role and position.
    """
    return roles.join(
        F.broadcast(special.select(F.col("artist_id").alias("artist"))), "artist", "left_anti"
    )


def collapse_repeats(roles: DataFrame) -> DataFrame:
    """One row per (credit, artist) when an artist is listed more than once.

    In/Out: (artist_credit, position, artist, role). Keeps the first position,
    and 'main' if any of the artist's slots is main.
    """
    return roles.groupBy("artist_credit", "artist").agg(
        F.min("position").alias("position"),
        F.when(F.max((F.col("role") == MAIN).cast("int")) == 1, F.lit(MAIN))
        .otherwise(F.lit(FEATURED))
        .alias("role"),
    )


def recording_credits(recording: DataFrame, roles: DataFrame) -> DataFrame:
    """Attach credit roles to recordings.

    In: recording(id, artist_credit), collapsed roles.
    Out: (recording_id long, artist_id long, position int, role), one row per
    (recording, artist).
    """
    return (
        recording.select(F.col("id").alias("recording_id"), "artist_credit")
        .join(roles, "artist_credit")
        .select(
            F.col("recording_id").cast("long"),
            F.col("artist").cast("long").alias("artist_id"),
            F.col("position").cast("int"),
            "role",
        )
    )


def run(cfg: Config, sample: float | None) -> dict:
    """Run the stage; return the logged counts."""
    pattern = cfg.get("credits.featuring_pattern")
    spark = get_spark("musin-clean-credits")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    special = spark.read.parquet(str(cfg.data_path("clean") / "special_artists"))
    recording = sample_by_hash(raw("recording"), "id", sample).select("id", "artist_credit")
    recording = recording.cache()
    acn = raw("artist_credit_name").join(
        recording.select("artist_credit").distinct(), "artist_credit"
    )

    roles = credit_roles(acn, pattern).cache()
    kept = drop_special(roles, special)
    collapsed = collapse_repeats(kept).cache()
    out = layer_path(cfg, "clean", sample) / "credits"
    recording_credits(recording, collapsed).write.mode("overwrite").parquet(str(out))
    credits = spark.read.parquet(str(out))

    def recs_with(df: DataFrame) -> int:
        return recording.join(df.select("artist_credit").distinct(), "artist_credit").count()

    per_rec = credits.groupBy("recording_id").agg(
        F.sum((F.col("role") == MAIN).cast("int")).alias("mains"),
        F.sum((F.col("role") == FEATURED).cast("int")).alias("featured"),
    )
    rec_stats = per_rec.agg(
        F.count("*").alias("recordings"),
        F.sum((F.col("featured") > 0).cast("long")).alias("with_featured"),
        F.sum(((F.col("featured") > 0) & (F.col("mains") == 0)).cast("long")).alias(
            "featured_no_main"
        ),
    ).collect()[0]
    row_stats = credits.agg(
        F.count("*").alias("rows"),
        F.sum((F.col("role") == FEATURED).cast("long")).alias("featured_rows"),
        F.countDistinct("artist_id").alias("artists"),
    ).collect()[0]
    n_rec = recording.count()
    counts = {
        "recordings_in": n_rec,
        "credit_name_rows_in": roles.count(),
        "recordings_with_featured_before_filter": recs_with(
            roles.filter(F.col("role") == FEATURED)
        ),
        "credit_name_rows_special_removed": roles.count() - kept.count(),
        "repeated_artist_slots_collapsed": kept.count() - collapsed.count(),
        "credit_rows_out": row_stats["rows"],
        "featured_rows_out": row_stats["featured_rows"],
        "distinct_artists_out": row_stats["artists"],
        "recordings_out": rec_stats["recordings"],
        "recordings_dropped_only_special": n_rec - rec_stats["recordings"],
        "recordings_with_featured_out": rec_stats["with_featured"],
        "recordings_featured_without_main": rec_stats["featured_no_main"],
    }
    log.info("sample fraction: %s -> %s", sample if sample else "full", out)
    for key, value in counts.items():
        log.info("  %-40s %s", key, value)

    problems = []
    dupes = credits.groupBy("recording_id", "artist_id").count().filter("count > 1").count()
    if dupes:
        problems.append(f"{dupes} duplicate (recording_id, artist_id)")
    leaked = credits.join(F.broadcast(special.select("artist_id")), "artist_id").count()
    if leaked:
        problems.append(f"{leaked} special-artist rows")
    bad_roles = credits.filter(~F.col("role").isin(MAIN, FEATURED)).count()
    if bad_roles:
        problems.append(f"{bad_roles} rows with an unknown role")
    for df in (recording, roles, collapsed):
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
