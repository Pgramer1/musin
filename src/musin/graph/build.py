"""Edge building, aggregation, and graph filters (F3.1, FR-09, FR-10, FR-11).

    python -m musin.graph.build --config config/pipeline.yaml [--sample 0.05]

Reads:  <clean>/{credits,contributions,recording_years}  (F2.2-F2.4; sample tree with --sample)
        $MUSIN_DATA/<paths.clean>/{artists,artist_genres,artist_primary_genre}  (F2.1, F2.5; full)
        $MUSIN_DATA/<paths.raw>/{l_artist_artist,link,link_type}  (F1.3, band membership)
Writes: <graph>/{nodes_artist,artist_genre,edges,edges_by_decade}  (PRD §8.1)
        <reports_dir>/tables/graph_build_summary.csv  (full mode only)
        <clean>/<graph> as in D-019: $MUSIN_DATA/sample/f<fraction>/... with --sample.

Per recording (PRD §8.2): main x main and featured x featured -> CO_CREDITED
(src_id < dst_id), main -> featured -> FEATURED, contributor -> each main
artist -> PRODUCED_FOR / PERFORMED_FOR. Before aggregating, the stage drops
(D-026): every edge of a recording with more than `filters.max_credit_size`
credited artists (FR-10), self-loops, edges touching an excluded artist
(`filters.exclude_artist_types`, D-021; classical if `filters.include_classical`
is false, OD1), edges between a group and its own member (FR-11), and repeats of
an artist pair on one recording: each recording counts once per pair, under
the billed type if any, else PRODUCED_FOR, else PERFORMED_FOR (D-028). Edges are
then aggregated per (src, dst, type) into distinct-recording weights, and the
node rule of PRD §8.3 (`recording_count >= filters.min_recordings` and at
least one edge) decides which artists and edges reach graph/.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from musin.clean.credits import FEATURED as ROLE_FEATURED
from musin.clean.credits import MAIN as ROLE_MAIN
from musin.common.config import Config, ConfigError, load_config
from musin.common.layers import layer_path
from musin.common.spark import get_spark

log = logging.getLogger("musin.graph.build")

CO_CREDITED, FEATURED = "CO_CREDITED", "FEATURED"
PRODUCED_FOR, PERFORMED_FOR = "PRODUCED_FOR", "PERFORMED_FOR"
EDGE_TYPES = (CO_CREDITED, FEATURED, PRODUCED_FOR, PERFORMED_FOR)

# FR-11 names the relationship; it is a MusicBrainz identifier, not a tunable.
MEMBER_OF_BAND = "member of band"
# Parent genre (D-024) removed when filters.include_classical is false (OD1).
CLASSICAL = "classical"

# Drop reasons, in the order they are applied (an edge counts under the first).
SELF_LOOP, EXCLUDED_TYPE, EXCLUDED_CLASSICAL, BAND_MEMBER, SAME_RECORDING_PAIR = (
    "self_loop",
    "excluded_artist_type",
    "excluded_classical",
    "band_member",
    "same_recording_pair",
)
# Which instance of an artist pair a recording keeps (D-028): billing first,
# then production, then performance. Lower wins.
PAIR_PRECEDENCE = {CO_CREDITED: 0, FEATURED: 0, PRODUCED_FOR: 1, PERFORMED_FOR: 2}
INSTANCE = ["recording_id", "src_id", "dst_id", "edge_type"]
EDGE_KEY = ["src_id", "dst_id", "edge_type"]
# The §8.1 tables this stage writes, in contract order.
GRAPH_TABLES = ("nodes_artist", "artist_genre", "edges", "edges_by_decade")


def credit_sizes(credits: DataFrame) -> DataFrame:
    """Credited artists per recording.

    In: clean/credits(recording_id, artist_id). Out: (recording_id, n_artists),
    one row per recording. clean/credits has one row per (recording, artist)
    with special artists already removed, so this is the distinct count.
    """
    return credits.groupBy("recording_id").agg(F.count("*").alias("n_artists"))


def credit_edges(credits: DataFrame) -> DataFrame:
    """Edge instances from billed credits (PRD §8.2).

    In: clean/credits(recording_id, artist_id, role), already limited to the
    recordings that may produce edges (FR-10 is applied by the caller, because
    the self-join is quadratic in the credit size).
    Out: (recording_id, src_id, dst_id, edge_type), one row per edge instance:
    main x main and featured x featured -> CO_CREDITED with src_id < dst_id;
    main -> featured -> FEATURED. No self-loops (one row per artist per recording).
    """
    left = credits.select(
        "recording_id", F.col("artist_id").alias("src_id"), F.col("role").alias("src_role")
    )
    right = credits.select(
        "recording_id", F.col("artist_id").alias("dst_id"), F.col("role").alias("dst_role")
    )
    edge_type = F.when(
        (F.col("src_role") == F.col("dst_role")) & (F.col("src_id") < F.col("dst_id")),
        F.lit(CO_CREDITED),
    ).when((F.col("src_role") == ROLE_MAIN) & (F.col("dst_role") == ROLE_FEATURED), F.lit(FEATURED))
    return (
        left.join(right, "recording_id")
        .select("recording_id", "src_id", "dst_id", edge_type.alias("edge_type"))
        .filter(F.col("edge_type").isNotNull())
    )


def contribution_edges(contributions: DataFrame, credits: DataFrame) -> DataFrame:
    """Edge instances from contribution credits (PRD §8.2).

    In: clean/contributions(recording_id, artist_id, edge_type),
    clean/credits(recording_id, artist_id, role).
    Out: (recording_id, src_id, dst_id, edge_type): contributor -> every MAIN
    artist of the recording. Self-loops (a contributor who is also a main
    artist) are kept here; mark_drops() labels them.
    """
    mains = credits.filter(F.col("role") == ROLE_MAIN).select(
        "recording_id", F.col("artist_id").alias("dst_id")
    )
    return (
        contributions.select("recording_id", F.col("artist_id").alias("src_id"), "edge_type")
        .join(mains, "recording_id")
        .select(*INSTANCE)
    )


def band_member_pairs(
    l_artist_artist: DataFrame, link: DataFrame, link_type: DataFrame
) -> DataFrame:
    """Unordered (group, member) pairs from "member of band" relationships (FR-11).

    In: raw l_artist_artist(link, entity0, entity1), raw link(id, link_type),
    raw link_type(id, name, entity_type0, entity_type1).
    Out: distinct (a, b) with a < b. Raises ValueError if the dump has no
    artist -> artist link type of that name, so a renamed type cannot silently
    disable the filter.
    """
    types = link_type.filter(
        (F.col("name") == MEMBER_OF_BAND)
        & (F.col("entity_type0") == "artist")
        & (F.col("entity_type1") == "artist")
    ).select(F.col("id").alias("link_type"))
    if types.limit(1).count() == 0:
        raise ValueError(f"no artist -> artist link type named '{MEMBER_OF_BAND}' in the dump")
    return (
        l_artist_artist.select("link", "entity0", "entity1")
        .join(link.select(F.col("id").alias("link"), "link_type"), "link")
        .join(F.broadcast(types), "link_type")
        .select(
            F.least("entity0", "entity1").cast("long").alias("a"),
            F.greatest("entity0", "entity1").cast("long").alias("b"),
        )
        .filter(F.col("a") != F.col("b"))
        .distinct()
    )


def excluded_artists(
    artists: DataFrame, primary: DataFrame, exclude_types: list[str], include_classical: bool
) -> DataFrame:
    """Artists left out of the graph, with the reason.

    In: clean/artists(artist_id, artist_type), clean/artist_primary_genre
    (artist_id, primary_genre), the `filters.exclude_artist_types` list and
    `filters.include_classical`.
    Out: (artist_id, reason): EXCLUDED_TYPE for an artist type in the list;
    EXCLUDED_CLASSICAL for primary genre 'classical' when classical is not
    included. An artist matching both is listed once, by type.
    """
    by_type = artists.filter(F.col("artist_type").isin(exclude_types)).select(
        "artist_id", F.lit(EXCLUDED_TYPE).alias("reason")
    )
    if include_classical:
        return by_type
    by_genre = (
        primary.filter(F.col("primary_genre") == CLASSICAL)
        .select("artist_id", F.lit(EXCLUDED_CLASSICAL).alias("reason"))
        .join(by_type.select("artist_id"), "artist_id", "left_anti")
    )
    return by_type.unionByName(by_genre)


def mark_drops(instances: DataFrame, excluded: DataFrame, band_pairs: DataFrame) -> DataFrame:
    """Label every edge instance with the first filter that removes it.

    In: (recording_id, src_id, dst_id, edge_type), excluded_artists output,
    band_member_pairs output.
    Out: the same rows plus `drop_reason`: SELF_LOOP, the exclusion reason of
    src (else dst), BAND_MEMBER for a group-member pair in either direction,
    SAME_RECORDING_PAIR when the recording already gives the same unordered
    pair an instance of higher PAIR_PRECEDENCE (D-028), or null if kept.
    Ties within one precedence level cannot occur for a kept pair: billing
    gives one instance per pair, and two contribution instances of one type
    in opposite directions need both artists billed as main.
    """
    src_x = excluded.select(F.col("artist_id").alias("src_id"), F.col("reason").alias("src_x"))
    dst_x = excluded.select(F.col("artist_id").alias("dst_id"), F.col("reason").alias("dst_x"))
    members = band_pairs.withColumn("band", F.lit(True))
    precedence = F.create_map(*[F.lit(x) for kv in PAIR_PRECEDENCE.items() for x in kv])
    per_pair = Window.partitionBy("recording_id", "a", "b").orderBy(
        precedence[F.col("edge_type")], "edge_type", "src_id"
    )
    reason = (
        F.when(F.col("src_id") == F.col("dst_id"), F.lit(SELF_LOOP))
        .when(F.col("src_x").isNotNull(), F.col("src_x"))
        .when(F.col("dst_x").isNotNull(), F.col("dst_x"))
        .when(F.col("band"), F.lit(BAND_MEMBER))
        .when(F.col("pair_rank") > 1, F.lit(SAME_RECORDING_PAIR))
    )
    return (
        instances.join(src_x, "src_id", "left")
        .join(dst_x, "dst_id", "left")
        .withColumn("a", F.least("src_id", "dst_id"))
        .withColumn("b", F.greatest("src_id", "dst_id"))
        .join(members, ["a", "b"], "left")
        .withColumn("pair_rank", F.row_number().over(per_pair))
        .select(*INSTANCE, reason.alias("drop_reason"))
    )


def with_years(instances: DataFrame, years: DataFrame) -> DataFrame:
    """Attach each instance's recording first release year.

    In: (recording_id, src_id, dst_id, edge_type), clean/recording_years
    (recording_id, first_year nullable). Out: the distinct instances plus
    `first_year` (null when the recording has no known year).
    """
    return instances.select(*INSTANCE).distinct().join(years, "recording_id", "left")


def aggregate_edges(instances: DataFrame) -> DataFrame:
    """Aggregate instances into the §8.1 `edges` table.

    In: with_years output. Out: (src_id long, dst_id long, edge_type, weight
    long = distinct recordings, first_year int, last_year int); the years are
    the min / max known first-release year, null if none is known.
    """
    return instances.groupBy(*EDGE_KEY).agg(
        F.countDistinct("recording_id").cast("long").alias("weight"),
        F.min("first_year").cast("int").alias("first_year"),
        F.max("first_year").cast("int").alias("last_year"),
    )


def decade_of(year: Column) -> Column:
    """First year of the decade, e.g. 1994 -> 1990."""
    return (F.floor(year / 10) * 10).cast("int")


def aggregate_by_decade(instances: DataFrame) -> DataFrame:
    """Aggregate instances into the §8.1 `edges_by_decade` table.

    In: with_years output. Out: (src_id, dst_id, edge_type, decade int,
    weight long), one row per decade with at least one shared recording of
    known year; recordings without a year are left out (D-011).
    """
    return (
        instances.filter(F.col("first_year").isNotNull())
        .withColumn("decade", decade_of(F.col("first_year")))
        .groupBy(*EDGE_KEY, "decade")
        .agg(F.countDistinct("recording_id").cast("long").alias("weight"))
    )


def recording_counts(credits: DataFrame, contributions: DataFrame) -> DataFrame:
    """Distinct recordings each artist is credited on, billed or as a contributor.

    In: clean/credits(recording_id, artist_id), clean/contributions
    (recording_id, artist_id). Out: (artist_id, recording_count long). Counted
    over every recording in the input, before any edge filter (D-027).
    """
    return (
        credits.select("recording_id", "artist_id")
        .unionByName(contributions.select("recording_id", "artist_id"))
        .groupBy("artist_id")
        .agg(F.countDistinct("recording_id").cast("long").alias("recording_count"))
    )


def apply_node_rule(edges: DataFrame, counts: DataFrame, min_recordings: int) -> DataFrame:
    """Keep edges whose two endpoints have recording_count >= min_recordings (PRD §8.3).

    In: aggregate_edges output, recording_counts output. Out: the kept edges,
    same columns. The artists of the graph are exactly the endpoints of the
    kept edges, so every node meets the threshold and has an edge.
    """
    eligible = counts.filter(F.col("recording_count") >= min_recordings).select("artist_id")
    return (
        edges.join(eligible.withColumnRenamed("artist_id", "src_id"), "src_id", "left_semi")
        .join(eligible.withColumnRenamed("artist_id", "dst_id"), "dst_id", "left_semi")
        .select(*edges.columns)
    )


def endpoints(edges: DataFrame) -> DataFrame:
    """Distinct artist ids at either end of an edge. In: (src_id, dst_id). Out: (artist_id)."""
    return (
        edges.select(F.col("src_id").alias("artist_id"))
        .unionByName(edges.select(F.col("dst_id").alias("artist_id")))
        .distinct()
    )


def node_table(
    node_ids: DataFrame, artists: DataFrame, counts: DataFrame, primary: DataFrame
) -> DataFrame:
    """The §8.1 `nodes_artist` table.

    In: (artist_id) of the graph, clean/artists, recording_counts output,
    clean/artist_primary_genre. Out: (artist_id, mbid, name, artist_type,
    area_name, begin_year, recording_count, primary_genre, genre_votes), one row
    per graph artist; primary_genre and genre_votes null if untagged.
    """
    return (
        node_ids.join(artists, "artist_id")
        .join(counts, "artist_id")
        .join(primary.select("artist_id", "primary_genre", "genre_votes"), "artist_id", "left")
        .select(
            F.col("artist_id").cast("long"),
            "mbid",
            "name",
            "artist_type",
            "area_name",
            F.col("begin_year").cast("int"),
            F.col("recording_count").cast("long"),
            "primary_genre",
            F.col("genre_votes").cast("long"),
        )
    )


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(cfg: Config, sample: float | None) -> dict:
    """Run the stage; return the logged counts."""
    max_size = int(cfg.get("filters.max_credit_size"))
    min_recordings = int(cfg.get("filters.min_recordings"))
    exclude_types = list(cfg.get("filters.exclude_artist_types"))
    include_classical = bool(cfg.get("filters.include_classical"))
    repo_root = cfg.source.resolve().parent.parent
    spark = get_spark("musin-graph-build")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    def full_clean(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("clean") / table))

    clean_in = layer_path(cfg, "clean", sample)
    credits = spark.read.parquet(str(clean_in / "credits"))
    contributions = spark.read.parquet(str(clean_in / "contributions"))
    years = spark.read.parquet(str(clean_in / "recording_years"))
    artists = full_clean("artists")
    primary = full_clean("artist_primary_genre")

    known_types = {r["artist_type"] for r in artists.select("artist_type").distinct().collect()}
    unknown = sorted(set(exclude_types) - known_types)
    if unknown:
        raise ConfigError(f"filters.exclude_artist_types: no artists of type {unknown}")
    if not include_classical and primary.filter(F.col("primary_genre") == CLASSICAL).count() == 0:
        raise ConfigError(f"include_classical is false but no artist has primary genre {CLASSICAL}")

    # FR-10: recordings with more credited artists than max_credit_size give no edges.
    sizes = credit_sizes(credits).cache()
    small = sizes.filter(F.col("n_artists") <= max_size).select("recording_id")
    small_credits = credits.join(small, "recording_id", "left_semi")
    instances = credit_edges(small_credits).unionByName(
        contribution_edges(contributions.join(small, "recording_id", "left_semi"), small_credits)
    )
    excluded = excluded_artists(artists, primary, exclude_types, include_classical).cache()
    band_pairs = band_member_pairs(raw("l_artist_artist"), raw("link"), raw("link_type")).cache()
    marked = mark_drops(instances, excluded, band_pairs).cache()

    kept = with_years(marked.filter(F.col("drop_reason").isNull()), years).cache()
    counts_df = recording_counts(credits, contributions).cache()
    edges = apply_node_rule(aggregate_edges(kept), counts_df, min_recordings)

    out = layer_path(cfg, "graph", sample)
    edges.write.mode("overwrite").parquet(str(out / "edges"))
    edges = spark.read.parquet(str(out / "edges")).cache()
    aggregate_by_decade(kept).join(edges.select(*EDGE_KEY), EDGE_KEY, "left_semi").write.mode(
        "overwrite"
    ).parquet(str(out / "edges_by_decade"))
    node_ids = endpoints(edges).cache()
    node_table(node_ids, artists, counts_df, primary).write.mode("overwrite").parquet(
        str(out / "nodes_artist")
    )
    full_clean("artist_genres").join(node_ids, "artist_id", "left_semi").select(
        F.col("artist_id").cast("long"), "parent_genre", F.col("share").cast("double")
    ).write.mode("overwrite").parquet(str(out / "artist_genre"))
    nodes = spark.read.parquet(str(out / "nodes_artist")).cache()
    by_decade = spark.read.parquet(str(out / "edges_by_decade"))
    genre_out = spark.read.parquet(str(out / "artist_genre"))

    # --- counts (aggregated frames only) -----------------------------------------
    size_stats = sizes.agg(
        F.count("*").alias("recordings"),
        F.sum((F.col("n_artists") > max_size).cast("long")).alias("large"),
        F.sum(
            F.when(F.col("n_artists") > max_size, F.col("n_artists") * (F.col("n_artists") - 1) / 2)
        )
        .cast("long")
        .alias("large_pairs"),
    ).collect()[0]
    large = sizes.filter(F.col("n_artists") > max_size).select("recording_id")
    by_reason = {
        (r["edge_type"], r["drop_reason"] or "kept"): r["count"]
        for r in marked.groupBy("edge_type", "drop_reason").count().collect()
    }
    reasons = [
        "kept",
        SELF_LOOP,
        EXCLUDED_TYPE,
        EXCLUDED_CLASSICAL,
        BAND_MEMBER,
        SAME_RECORDING_PAIR,
    ]
    dropped_type = marked.filter(F.col("drop_reason") == EXCLUDED_TYPE)
    type_partners = (
        dropped_type.select(F.col("src_id").alias("artist_id"))
        .unionByName(dropped_type.select(F.col("dst_id").alias("artist_id")))
        .distinct()
        .join(excluded.select("artist_id"), "artist_id", "left_anti")
    )
    n_pre_node_rule = aggregate_edges(kept).count()
    edge_stats = {
        r["edge_type"]: r
        for r in edges.groupBy("edge_type")
        .agg(
            F.count("*").alias("edges"),
            F.sum("weight").alias("weight"),
            F.sum(F.col("first_year").isNull().cast("long")).alias("no_year"),
        )
        .collect()
    }
    node_stats = nodes.agg(
        F.count("*").alias("nodes"),
        F.count("primary_genre").alias("with_genre"),
        F.sum((F.col("primary_genre") == CLASSICAL).cast("long")).alias("classical"),
    ).collect()[0]
    classical_ids = nodes.filter(F.col("primary_genre") == CLASSICAL).select("artist_id")
    edges_classical = edges.join(
        classical_ids.withColumnRenamed("artist_id", "src_id"), "src_id", "left_semi"
    ).unionByName(
        edges.join(classical_ids.withColumnRenamed("artist_id", "dst_id"), "dst_id", "left_semi")
    )
    labelled = nodes.filter(F.col("primary_genre").isNotNull()).select("artist_id")
    n_edges = edges.count()
    counts = {
        "recordings_in": size_stats["recordings"],
        f"recordings_over_{max_size}_artists": size_stats["large"],
        f"pair_instances_skipped_over_{max_size}": size_stats["large_pairs"],
        f"contribution_rows_skipped_over_{max_size}": contributions.join(
            large, "recording_id", "left_semi"
        ).count(),
        "excluded_artists_by_type": excluded.filter(F.col("reason") == EXCLUDED_TYPE).count(),
        "excluded_artists_classical": excluded.filter(
            F.col("reason") == EXCLUDED_CLASSICAL
        ).count(),
        "band_member_pairs": band_pairs.count(),
        **{
            f"instances_{t}_{reason}": by_reason.get((t, reason), 0)
            for t in EDGE_TYPES
            for reason in reasons
        },
        "edges_touching_excluded_type": dropped_type.select(*EDGE_KEY).distinct().count(),
        "partners_of_excluded_type_not_in_graph": type_partners.join(
            nodes.select("artist_id"), "artist_id", "left_anti"
        ).count(),
        "edges_before_node_rule": n_pre_node_rule,
        "edges_removed_by_min_recordings": n_pre_node_rule - n_edges,
        "artists_with_recording_count": counts_df.count(),
        "artists_only_contribution_credits": counts_df.join(
            credits.select("artist_id").distinct(), "artist_id", "left_anti"
        ).count(),
        "nodes": node_stats["nodes"],
        "nodes_with_primary_genre": node_stats["with_genre"],
        "nodes_primary_classical": node_stats["classical"],
        "nodes_only_via_contributions": nodes.join(
            credits.select("artist_id").distinct(), "artist_id", "left_anti"
        ).count(),
        "edges": n_edges,
        **{f"edges_{t}": edge_stats[t]["edges"] if t in edge_stats else 0 for t in EDGE_TYPES},
        **{f"weight_{t}": edge_stats[t]["weight"] if t in edge_stats else 0 for t in EDGE_TYPES},
        "edges_without_year": sum(r["no_year"] for r in edge_stats.values()),
        "edges_touching_classical": edges_classical.distinct().count(),
        "edges_both_ends_labelled": edges.join(
            labelled.withColumnRenamed("artist_id", "src_id"), "src_id", "left_semi"
        )
        .join(labelled.withColumnRenamed("artist_id", "dst_id"), "dst_id", "left_semi")
        .count(),
        "edges_by_decade_rows": by_decade.count(),
        "artist_genre_rows": genre_out.count(),
    }
    log.info("sample fraction: %s -> %s", sample if sample else "full", out)
    for key, value in counts.items():
        log.info("  %-48s %s", key, value)
    neighbours = (
        edges.select(F.col("src_id").alias("artist_id"), F.col("dst_id").alias("other"))
        .unionByName(
            edges.select(F.col("dst_id").alias("artist_id"), F.col("src_id").alias("other"))
        )
        .groupBy("artist_id")
        .agg(F.countDistinct("other").alias("collaborators"))
    )
    top = (
        neighbours.join(
            nodes.select("artist_id", "name", "artist_type", "primary_genre"), "artist_id"
        )
        .orderBy(F.desc("collaborators"), "artist_id")
        .limit(15)
        .collect()
    )
    log.info("  top artists by distinct collaborators (all edge types):")
    for r in top:
        log.info(
            "    %7d  %s (%s, %s)",
            r["collaborators"],
            r["name"],
            r["artist_type"],
            r["primary_genre"],
        )

    if sample is None:
        tables = repo_root / cfg.get("reports_dir") / "tables"
        write_table(
            tables / "graph_build_summary.csv",
            [{"metric": k, "value": v} for k, v in counts.items()],
        )

    # --- stage quality checks (F3.3 adds the full graph-layer suite) -------------
    problems = []
    if edges.filter(F.col("src_id") == F.col("dst_id")).count():
        problems.append("self-loops in edges")
    if edges.groupBy(*EDGE_KEY).count().filter("count > 1").count():
        problems.append("duplicate (src_id, dst_id, edge_type)")
    if edges.filter(
        (F.col("edge_type") == CO_CREDITED) & (F.col("src_id") >= F.col("dst_id"))
    ).count():
        problems.append("CO_CREDITED edges with src_id >= dst_id")
    if node_ids.join(nodes, "artist_id", "left_anti").count():
        problems.append("edge endpoints missing from nodes_artist")
    if nodes.filter(F.col("recording_count") < min_recordings).count():
        problems.append("nodes below min_recordings")
    if nodes.join(excluded, "artist_id", "left_semi").count():
        problems.append("excluded artists in nodes_artist")
    if edges.filter(F.col("weight") < 1).count():
        problems.append("edges with weight < 1")
    repeated = (
        kept.groupBy("recording_id", F.least("src_id", "dst_id"), F.greatest("src_id", "dst_id"))
        .count()
        .filter("count > 1")
        .count()
    )
    if repeated:
        problems.append(f"{repeated} (recording, artist pair) counted more than once (D-028)")
    for df in (sizes, excluded, band_pairs, marked, kept, counts_df, edges, node_ids, nodes):
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
