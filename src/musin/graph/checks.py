"""Graph-layer quality checks (F3.3, FR-13).

    python -m musin.graph.checks --config config/pipeline.yaml [--sample 0.05] [--variant <name>]
        [--accept-changes]

Reads:  <graph>/{nodes_artist,artist_genre,edges,edges_by_decade}  (F3.1; sample tree with --sample)
        <genres.mapping_file>  (the parent-genre list, D-024)
Writes: <graph>/_checks.json  the last PASSING check of this folder: row counts,
                              content fingerprints, and every check result
        <reports_dir>/tables/graph_checks.csv  (full mode only; variants: tables/variants/<name>/)
With --variant <name> the checks run on that variant graph (D-035) with its
config overrides, and the variant folder keeps its own run-to-run baseline.
Exits with status 1 if a check fails; _checks.json is then left unchanged.

The suite reads only the written tables, so it is independent of the build
code and its inline checks. It covers the four §8.1 tables (schema, keys,
value ranges, cross-table references), the PRD §8.3 node rule, the §8.1
`edges_by_decade` invariant (D-011) and its stronger first/last-decade form,
and the consistency of `primary_genre` with `artist_genre`. Run-to-run
(RULES §6): every table's row count, and the edge count per type, must lie
within `checks.max_row_change` (relative) of the last passing check of the
same folder. A deliberate change (new config, a G1 decision) is accepted with
--accept-changes, and must be explained in a decision entry or feature doc.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from musin.clean.genres import load_mapping
from musin.common.config import Config, load_config
from musin.common.layers import graph_name, layer_path, results_dir
from musin.common.provenance import fingerprint, repo_commit
from musin.common.spark import get_spark
from musin.graph.build import (
    CO_CREDITED,
    EDGE_KEY,
    EDGE_TYPES,
    GRAPH_TABLES,
    decade_of,
    endpoints,
    write_table,
)

log = logging.getLogger("musin.graph.checks")

RESULT = "_checks.json"
SHARE_TOLERANCE = 1e-9  # floating-point slack for "shares sum to 1"

SCHEMAS: dict[str, list[tuple[str, str]]] = {  # PRD §8.1, in column order
    "nodes_artist": [
        ("artist_id", "bigint"),
        ("mbid", "string"),
        ("name", "string"),
        ("artist_type", "string"),
        ("area_name", "string"),
        ("begin_year", "int"),
        ("recording_count", "bigint"),
        ("primary_genre", "string"),
        ("genre_votes", "bigint"),
    ],
    "artist_genre": [("artist_id", "bigint"), ("parent_genre", "string"), ("share", "double")],
    "edges": [
        ("src_id", "bigint"),
        ("dst_id", "bigint"),
        ("edge_type", "string"),
        ("weight", "bigint"),
        ("first_year", "int"),
        ("last_year", "int"),
    ],
    "edges_by_decade": [
        ("src_id", "bigint"),
        ("dst_id", "bigint"),
        ("edge_type", "string"),
        ("decade", "int"),
        ("weight", "bigint"),
    ],
}


@dataclass
class Check:
    """One check: how many rows (or tables) violate it. 0 = passed."""

    table: str
    name: str
    violations: int
    detail: str = ""
    accepted: bool = False  # a run-to-run change accepted with --accept-changes

    @property
    def passed(self) -> bool:
        return self.violations == 0 or self.accepted

    @property
    def status(self) -> str:
        if self.violations == 0:
            return "pass"
        return "accepted" if self.accepted else "FAIL"


def count_rows(df: DataFrame, table: str, conditions: dict[str, Column]) -> list[Check]:
    """One pass over `df`: a Check per named condition, counting rows where it is true."""
    row = df.agg(
        *[F.sum(F.coalesce(c, F.lit(False)).cast("long")).alias(n) for n, c in conditions.items()]
    ).collect()[0]
    return [Check(table, n, row[n] or 0) for n in conditions]


def duplicates(df: DataFrame, key: list[str]) -> int:
    """Rows beyond the first for each key value."""
    return (
        df.groupBy(*key).count().filter("count > 1").agg(F.sum(F.col("count") - 1)).collect()[0][0]
        or 0
    )


def check_schema(table: str, df: DataFrame) -> Check:
    """Column names, order, and types against PRD §8.1."""
    got = [(n, t) for n, t in df.dtypes]
    ok = got == SCHEMAS[table]
    return Check(table, "schema", 0 if ok else 1, "" if ok else f"got {got}")


def check_nodes(
    nodes: DataFrame,
    edges: DataFrame,
    min_recordings: int,
    excluded_types: list[str],
    parents: set[str],
) -> list[Check]:
    """nodes_artist: keys, node rule (PRD §8.3), exclusions (D-021), genre fields.

    In: §8.1 nodes_artist and edges, `filters.min_recordings`,
    `filters.exclude_artist_types`, the parent-genre names. Out: Checks.
    """
    t = "nodes_artist"
    pg = F.col("primary_genre")
    checks = count_rows(
        nodes,
        t,
        {
            "null_artist_id": F.col("artist_id").isNull(),
            "null_mbid_or_name": F.col("mbid").isNull() | F.col("name").isNull(),
            "below_min_recordings": F.coalesce(
                F.col("recording_count") < min_recordings, F.lit(True)
            ),
            "excluded_artist_type": F.col("artist_type").isin(excluded_types)
            if excluded_types
            else F.lit(False),
            "unknown_primary_genre": pg.isNotNull() & ~pg.isin(sorted(parents)),
            "genre_votes_mismatch": pg.isNull() != F.col("genre_votes").isNull(),
            "genre_votes_not_positive": F.col("genre_votes") <= 0,
        },
    )
    checks.append(Check(t, "duplicate_artist_id", duplicates(nodes, ["artist_id"])))
    checks.append(
        Check(t, "duplicate_mbid", duplicates(nodes.filter("mbid IS NOT NULL"), ["mbid"]))
    )
    checks.append(
        Check(
            t, "node_without_edge", nodes.join(endpoints(edges), "artist_id", "left_anti").count()
        )
    )
    return checks


def check_edges(edges: DataFrame, nodes: DataFrame, min_year: int, max_year: int) -> list[Check]:
    """edges: keys, types, PRD §8.2 orientation, weights, years, endpoints.

    In: §8.1 edges and nodes_artist, the plausible year range (D-023).
    Out: Checks.
    """
    t = "edges"
    fy, ly = F.col("first_year"), F.col("last_year")
    checks = count_rows(
        edges,
        t,
        {
            "null_key": F.col("src_id").isNull()
            | F.col("dst_id").isNull()
            | F.col("edge_type").isNull(),
            "self_loop": F.col("src_id") == F.col("dst_id"),
            "unknown_edge_type": ~F.col("edge_type").isin(*EDGE_TYPES),
            "co_credited_not_ordered": (F.col("edge_type") == CO_CREDITED)
            & (F.col("src_id") >= F.col("dst_id")),
            "weight_below_1": F.coalesce(F.col("weight") < 1, F.lit(True)),
            "year_null_mismatch": fy.isNull() != ly.isNull(),
            "first_year_after_last": fy > ly,
            "year_out_of_range": (fy < min_year) | (ly > max_year),
        },
    )
    checks.append(Check(t, "duplicate_key", duplicates(edges, EDGE_KEY)))
    ids = nodes.select("artist_id")
    missing = (
        edges.join(ids.withColumnRenamed("artist_id", "src_id"), "src_id", "left_anti")
        .unionByName(
            edges.join(ids.withColumnRenamed("artist_id", "dst_id"), "dst_id", "left_anti")
        )
        .distinct()
        .count()
    )
    checks.append(Check(t, "endpoint_not_a_node", missing))
    return checks


def check_decades(by_decade: DataFrame, edges: DataFrame) -> list[Check]:
    """edges_by_decade against edges (PRD §8.1, D-011).

    Besides the §8.1 invariant (decade weights summed <= edge weight), every
    dated edge must have decade rows whose first and last decade are those of
    its first_year and last_year, an undated edge must have none, and no row
    may fall outside the edge's year span. In: §8.1 tables. Out: Checks.
    """
    t = "edges_by_decade"
    checks = count_rows(
        by_decade,
        t,
        {
            "null_key": F.col("src_id").isNull()
            | F.col("dst_id").isNull()
            | F.col("edge_type").isNull()
            | F.col("decade").isNull(),
            "decade_not_multiple_of_10": F.col("decade") % 10 != 0,
            "weight_below_1": F.coalesce(F.col("weight") < 1, F.lit(True)),
        },
    )
    checks.append(Check(t, "duplicate_key", duplicates(by_decade, [*EDGE_KEY, "decade"])))
    e = edges.select(*EDGE_KEY, F.col("weight").alias("edge_weight"), "first_year", "last_year")
    rows = by_decade.join(e, EDGE_KEY, "left")
    checks += count_rows(
        rows,
        t,
        {
            "row_without_edge": F.col("edge_weight").isNull(),
            "decade_outside_edge_years": F.col("edge_weight").isNotNull()
            & F.coalesce(
                (F.col("decade") < decade_of(F.col("first_year")))
                | (F.col("decade") > decade_of(F.col("last_year"))),
                F.lit(True),  # a row for an edge without a known year
            ),
        },
    )
    per_edge = by_decade.groupBy(*EDGE_KEY).agg(
        F.sum("weight").alias("decade_weight"),
        F.min("decade").alias("min_decade"),
        F.max("decade").alias("max_decade"),
    )
    dated = e.filter(F.col("first_year").isNotNull()).join(per_edge, EDGE_KEY, "left")
    checks += count_rows(
        dated,
        t,
        {
            "invariant_decades_exceed_weight": F.col("decade_weight") > F.col("edge_weight"),
            "dated_edge_without_decades": F.col("decade_weight").isNull(),
            "first_decade_mismatch": F.col("min_decade") != decade_of(F.col("first_year")),
            "last_decade_mismatch": F.col("max_decade") != decade_of(F.col("last_year")),
        },
    )
    return checks


def check_genres(artist_genre: DataFrame, nodes: DataFrame, parents: set[str]) -> list[Check]:
    """artist_genre: keys, shares (sum to 1), parents, and primary-genre consistency.

    `primary_genre` must be the highest share with the alphabetical tie-break
    (PRD §8.1), and null exactly when the artist has no profile.
    In: §8.1 artist_genre and nodes_artist, parent-genre names. Out: Checks.
    """
    t = "artist_genre"
    checks = count_rows(
        artist_genre,
        t,
        {
            "null_key": F.col("artist_id").isNull() | F.col("parent_genre").isNull(),
            "share_out_of_range": F.coalesce(
                (F.col("share") <= 0) | (F.col("share") > 1), F.lit(True)
            ),
            "unknown_parent_genre": ~F.col("parent_genre").isin(sorted(parents)),
        },
    )
    checks.append(
        Check(t, "duplicate_key", duplicates(artist_genre, ["artist_id", "parent_genre"]))
    )
    checks.append(
        Check(
            t,
            "artist_not_a_node",
            artist_genre.join(nodes.select("artist_id"), "artist_id", "left_anti").count(),
        )
    )
    sums = artist_genre.groupBy("artist_id").agg(F.sum("share").alias("s"))
    checks.append(
        Check(
            t,
            "shares_not_summing_to_1",
            sums.filter(F.abs(F.col("s") - 1) > SHARE_TOLERANCE).count(),
        )
    )
    rank = Window.partitionBy("artist_id").orderBy(F.desc("share"), "parent_genre")
    top = (
        artist_genre.withColumn("r", F.row_number().over(rank))
        .filter("r = 1")
        .select("artist_id", F.col("parent_genre").alias("top_genre"))
    )
    mismatch = (
        nodes.select("artist_id", "primary_genre")
        .join(top, "artist_id", "left")
        .filter(~F.col("primary_genre").eqNullSafe(F.col("top_genre")))
        .count()
    )
    checks.append(Check("nodes_artist", "primary_genre_not_top_share", mismatch))
    return checks


def row_changes(
    current: dict[str, int], baseline: dict[str, int] | None, max_change: float, accept: bool
) -> list[Check]:
    """Run-to-run row counts (RULES §6) against the last passing check.

    A count whose relative change exceeds `max_change` is a violation;
    `accept` marks such violations as accepted. Without a baseline (first
    check of a folder) every count passes, with a note.
    """
    checks = []
    for key, now in current.items():
        if baseline is None or key not in baseline:
            checks.append(Check("run_to_run", key, 0, f"no previous count; now {now}"))
            continue
        before = baseline[key]
        change = abs(now - before) / before if before else (0.0 if now == 0 else float("inf"))
        bad = change > max_change
        checks.append(
            Check(
                "run_to_run",
                key,
                1 if bad else 0,
                f"{before} -> {now} ({(now - before) / before:+.2%})" if before else f"0 -> {now}",
                accepted=bad and accept,
            )
        )
    return checks


def run(cfg: Config, sample: float | None, accept_changes: bool) -> list[Check]:
    """Run every check on one graph folder; raise RuntimeError if any fails."""
    repo_root = cfg.source.resolve().parent.parent
    parents = {p for p in load_mapping(repo_root / cfg.get("genres.mapping_file")).values() if p}
    min_recordings = int(cfg.get("filters.min_recordings"))
    excluded = list(cfg.get("filters.exclude_artist_types"))
    min_year = int(cfg.get("years.min_release_year"))
    max_year = int(str(cfg.get("snapshot_date"))[:4])
    max_change = float(cfg.get("checks.max_row_change"))
    root = layer_path(cfg, "graph", sample)
    spark = get_spark("musin-graph-checks")

    tables = {t: spark.read.parquet(str(root / t)) for t in GRAPH_TABLES}
    for df in tables.values():
        df.cache()
    nodes, edges = tables["nodes_artist"], tables["edges"]

    checks = [check_schema(t, df) for t, df in tables.items()]
    checks += check_nodes(nodes, edges, min_recordings, excluded, parents)
    checks += check_edges(edges, nodes, min_year, max_year)
    checks += check_decades(tables["edges_by_decade"], edges)
    checks += check_genres(tables["artist_genre"], nodes, parents)

    prints = {t: fingerprint(df) for t, df in tables.items()}
    counts = {t: fp["rows"] for t, fp in prints.items()}
    by_type = {r["edge_type"]: r["count"] for r in edges.groupBy("edge_type").count().collect()}
    counts.update({f"edges_{t}": by_type.get(t, 0) for t in EDGE_TYPES})
    result_file = root / RESULT
    baseline = json.loads(result_file.read_text())["counts"] if result_file.is_file() else None
    checks += row_changes(counts, baseline, max_change, accept_changes)

    log.info("graph %s: %s", graph_name(sample, cfg.variant), root)
    for c in checks:
        log.info("  %-5s %-16s %-34s %10d  %s", c.status, c.table, c.name, c.violations, c.detail)
    if sample is None:
        write_table(
            results_dir(cfg, graph_name(None, cfg.variant)) / "graph_checks.csv",
            [
                {
                    "table": c.table,
                    "check": c.name,
                    "violations": c.violations,
                    "status": c.status,
                    "detail": c.detail,
                }
                for c in checks
            ],
        )
    failed = [c for c in checks if not c.passed]
    if not failed:
        result = {
            "checked_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "repo_commit": repo_commit(repo_root),
            "sample_fraction": sample,
            "variant": cfg.variant,
            "variant_overrides": cfg.overrides,
            "max_row_change": max_change,
            "changes_accepted": [c.name for c in checks if c.accepted],
            "counts": counts,
            "fingerprints": {t: fp["fingerprint"] for t, fp in prints.items()},
            "checks": [asdict(c) for c in checks],
        }
        result_file.write_text(json.dumps(result, indent=2) + "\n")
        log.info("all %d checks passed -> %s", len(checks), result_file)
    for df in tables.values():
        df.unpersist()
    spark.stop()
    if failed:
        raise RuntimeError(
            f"{len(failed)} graph checks failed: "
            + "; ".join(f"{c.table}.{c.name}={c.violations} {c.detail}".strip() for c in failed)
            + ("" if accept_changes else " (a deliberate count change: --accept-changes)")
        )
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--sample", type=float, help="check the sample graph of this fraction")
    parser.add_argument("--variant", help="check the variant graph defined under variants.<name>")
    parser.add_argument(
        "--accept-changes",
        action="store_true",
        help="accept row-count changes beyond checks.max_row_change (explain them in the docs)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    try:
        run(load_config(args.config, args.variant), args.sample, args.accept_changes)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
