"""Contribution relationship extraction (F2.3, FR-06).

    python -m musin.clean.contributions --config config/pipeline.yaml [--sample 0.05]

Reads:  $MUSIN_DATA/<paths.raw>/{recording,l_artist_recording,link,link_type}  (F1.3)
        and, if filters.use_release_level_credits, {l_artist_release,track,medium}
        $MUSIN_DATA/<paths.clean>/special_artists/  (F2.1, always full)
Writes: <clean>/contributions/  (recording_id, artist_id, edge_type, link_types, level)
        <clean> as in D-019 (sample tree with --sample).

An artist -> recording relationship whose link type name is in
`contributions.produced_for` becomes a PRODUCED_FOR contribution, one in
`contributions.performed_for` a PERFORMED_FOR contribution (PRD §8.2). F3.1
turns each into an edge from the contributor to every main artist of the
recording. Every configured name must exist as a link type, or the stage fails.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.common.config import Config, ConfigError, load_config
from musin.common.layers import layer_path
from musin.common.sampling import sample_by_hash
from musin.common.spark import get_spark

log = logging.getLogger("musin.clean.contributions")

EDGE_TYPES = {"produced_for": "PRODUCED_FOR", "performed_for": "PERFORMED_FOR"}
RECORDING, RELEASE = "recording", "release"


def allowlist(cfg: Config) -> dict[str, str]:
    """{link type name: edge type} from the `contributions.*` config lists.

    Raises ConfigError if a name is listed under both edge types or a list is empty.
    """
    names: dict[str, str] = {}
    for key, edge_type in EDGE_TYPES.items():
        listed = cfg.get(f"contributions.{key}")
        if not listed:
            raise ConfigError(f"contributions.{key} is empty")
        for name in listed:
            if name in names:
                raise ConfigError(f"link type '{name}' is listed under two contribution types")
            names[name] = edge_type
    return names


def resolve_link_types(link_type: DataFrame, names: dict[str, str], target: str) -> DataFrame:
    """Allowlisted artist -> `target` link types.

    In: raw link_type(id, name, entity_type0, entity_type1), {name: edge_type}.
    Out: (link_type id, name, edge_type). Raises ConfigError if a configured
    name has no artist -> `target` link type in the dump.
    """
    found = (
        link_type.filter(
            (F.col("entity_type0") == "artist")
            & (F.col("entity_type1") == target)
            & F.col("name").isin(list(names))
        )
        .select(F.col("id").alias("link_type"), "name")
        .collect()  # a handful of rows
    )
    missing = sorted(set(names) - {r["name"] for r in found})
    if missing:
        raise ConfigError(f"no artist -> {target} link type named {missing} in the dump")
    spark = link_type.sparkSession
    return spark.createDataFrame(
        [(r["link_type"], r["name"], names[r["name"]]) for r in found],
        "link_type int, name string, edge_type string",
    )


def relationships(rel: DataFrame, link: DataFrame, types: DataFrame) -> DataFrame:
    """Allowlisted relationships of one l_artist_<target> table.

    In: l_artist_<target>(link, entity0, entity1), raw link(id, link_type),
    resolve_link_types output. Out: (artist, target, name, edge_type).
    """
    return (
        rel.select("link", F.col("entity0").alias("artist"), F.col("entity1").alias("target"))
        .join(link.select(F.col("id").alias("link"), "link_type"), "link")
        .join(F.broadcast(types), "link_type")
        .select("artist", "target", "name", "edge_type")
    )


def release_to_recordings(track: DataFrame, medium: DataFrame) -> DataFrame:
    """(release, recording) pairs: every recording that appears on a release.

    In: raw track(recording, medium), raw medium(id, release). Out: distinct
    (release, recording).
    """
    return (
        track.select("recording", "medium")
        .join(medium.select(F.col("id").alias("medium"), "release"), "medium")
        .select("release", "recording")
        .distinct()
    )


def collapse(contribs: DataFrame) -> DataFrame:
    """One row per (recording, artist, edge type).

    In: (recording_id, artist_id, name, edge_type, level).
    Out: (recording_id long, artist_id long, edge_type, link_types array<string>
    sorted, level): level is 'recording' if any source row is recording-level.
    """
    return (
        contribs.groupBy("recording_id", "artist_id", "edge_type")
        .agg(
            F.array_sort(F.collect_set("name")).alias("link_types"),
            F.when(F.max((F.col("level") == RECORDING).cast("int")) == 1, F.lit(RECORDING))
            .otherwise(F.lit(RELEASE))
            .alias("level"),
        )
        .select(
            F.col("recording_id").cast("long"),
            F.col("artist_id").cast("long"),
            "edge_type",
            "link_types",
            "level",
        )
    )


def run(cfg: Config, sample: float | None) -> dict:
    """Run the stage; return the logged counts."""
    names = allowlist(cfg)
    use_release = bool(cfg.get("filters.use_release_level_credits"))
    spark = get_spark("musin-clean-contributions")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    special = spark.read.parquet(str(cfg.data_path("clean") / "special_artists"))
    recordings = sample_by_hash(raw("recording"), "id", sample).select(
        F.col("id").alias("recording_id")
    )
    recordings = recordings.cache()
    link = raw("link")

    rec_types = resolve_link_types(raw("link_type"), names, RECORDING)
    rec_rels = (
        relationships(raw("l_artist_recording"), link, rec_types)
        .withColumnRenamed("target", "recording_id")
        .join(recordings, "recording_id")
        .select("recording_id", F.col("artist").alias("artist_id"), "name", "edge_type")
        .withColumn("level", F.lit(RECORDING))
    )
    parts = [rec_rels]
    if use_release:
        rel_types = resolve_link_types(raw("link_type"), names, RELEASE)
        rel_rels = (
            relationships(raw("l_artist_release"), link, rel_types)
            .withColumnRenamed("target", "release")
            .join(release_to_recordings(raw("track"), raw("medium")), "release")
            .withColumnRenamed("recording", "recording_id")
            .join(recordings, "recording_id")
            .select("recording_id", F.col("artist").alias("artist_id"), "name", "edge_type")
            .withColumn("level", F.lit(RELEASE))
        )
        parts.append(rel_rels)
    contribs = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    contribs = contribs.cache()
    kept = contribs.join(F.broadcast(special.select(F.col("artist_id"))), "artist_id", "left_anti")

    out = layer_path(cfg, "clean", sample) / "contributions"
    collapse(kept).write.mode("overwrite").parquet(str(out))
    result = spark.read.parquet(str(out))

    by_name = {
        f"relationships_{r['level']}_{r['name']}": r["count"]
        for r in contribs.groupBy("level", "name").count().collect()
    }
    stats = {
        f"{r['edge_type']}_{k}": r[k]
        for r in result.groupBy("edge_type")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("recording_id").alias("recordings"),
            F.countDistinct("artist_id").alias("artists"),
        )
        .collect()
        for k in ("rows", "recordings", "artists")
    }
    n_contribs, n_kept = contribs.count(), kept.count()
    counts = {
        "recordings_in": recordings.count(),
        "release_level_credits": use_release,
        **dict(sorted(by_name.items())),
        "relationships_matched": n_contribs,
        "relationships_special_removed": n_contribs - n_kept,
        "rows_out": result.count(),
        "rows_from_release_level_only": result.filter(F.col("level") == RELEASE).count(),
        "recordings_with_any_contribution": result.select("recording_id").distinct().count(),
        **dict(sorted(stats.items())),
    }
    log.info("sample fraction: %s -> %s", sample if sample else "full", out)
    for key, value in counts.items():
        log.info("  %-45s %s", key, value)

    problems = []
    dupes = (
        result.groupBy("recording_id", "artist_id", "edge_type").count().filter("count > 1").count()
    )
    if dupes:
        problems.append(f"{dupes} duplicate (recording_id, artist_id, edge_type)")
    if result.join(F.broadcast(special.select("artist_id")), "artist_id").count():
        problems.append("special artists in output")
    if result.filter(~F.col("edge_type").isin(*EDGE_TYPES.values())).count():
        problems.append("unknown edge_type")
    for df in (recordings, contribs):
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
