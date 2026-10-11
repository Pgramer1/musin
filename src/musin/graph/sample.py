"""Dev sample graph: the recording-level chain for one fraction (F3.2, FR-12).

    python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05 [--accept-changes]

Runs, with the same --sample fraction, every stage that reads recordings:
clean.credits (F2.2), clean.contributions (F2.3), clean.years (F2.4), and
graph.build (F3.1), followed by the graph checks (F3.3; --accept-changes is passed
on). Everything is written under $MUSIN_DATA/sample/f<fraction>/
(D-019); full-mode data is never touched. Artist-level inputs (clean/artists,
special_artists, artist_genres, artist_primary_genre, F2.1 and F2.5) are not
sampled and must already exist in the full clean layer.

Afterwards it writes:
  <sample>/graph/_manifest.json  build settings, row counts and order-independent
                                 content fingerprints of the four graph tables
                                 (equal fingerprints = identical tables)
  <reports_dir>/tables/sample_f<fraction>_vs_full.csv
                                 structural comparison with the full graph, if
                                 $MUSIN_DATA/<paths.graph> exists
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.clean import contributions, credits, years
from musin.common.config import Config, ConfigError, load_config
from musin.common.layers import layer_path, sample_tag
from musin.common.provenance import fingerprint, repo_commit
from musin.common.spark import get_spark
from musin.graph import build, checks
from musin.graph.build import EDGE_TYPES, GRAPH_TABLES, write_table

log = logging.getLogger("musin.graph.sample")

FULL_ARTIST_TABLES = ("artists", "special_artists", "artist_genres", "artist_primary_genre")
# Config sections whose values change the graph; copied into the manifest.
RESULT_SECTIONS = ("snapshot_date", "filters", "credits", "contributions", "genres", "years")
MANIFEST = "_manifest.json"


def degree_quantiles(edges: DataFrame) -> dict[str, float]:
    """Distinct collaborators per artist, all edge types (the COLLAB pairs, D-017).

    In: edges(src_id, dst_id). Out: {"degree_mean", "degree_p50", "degree_p90",
    "degree_p99", "degree_max"}; exact percentiles (the degree table is one
    aggregated row per artist; an approximate p99 can be off by a whole 1 %).
    """
    pairs = edges.select(
        F.least("src_id", "dst_id").alias("a"), F.greatest("src_id", "dst_id").alias("b")
    ).distinct()
    deg = (
        pairs.select(F.col("a").alias("artist_id"))
        .unionByName(pairs.select(F.col("b").alias("artist_id")))
        .groupBy("artist_id")
        .count()
    )
    row = deg.agg(
        F.mean("count").alias("mean"),
        F.expr("percentile(count, array(0.5, 0.9, 0.99))").alias("q"),
        F.max("count").alias("max"),
    ).collect()[0]
    return {
        "degree_mean": round(row["mean"], 4),
        "degree_p50": row["q"][0],
        "degree_p90": row["q"][1],
        "degree_p99": row["q"][2],
        "degree_max": row["max"],
    }


def graph_profile(nodes: DataFrame, edges: DataFrame, by_decade: DataFrame) -> dict[str, float]:
    """Structural metrics of one graph build, for comparing a sample with the full graph.

    In: §8.1 nodes_artist, edges, edges_by_decade. Out: {metric: value}:
    sizes, edge-type mix (by edges and by weight), genre-label coverage,
    weight-1 share, degree quantiles, and each decade's share of the total
    edge weight (the remainder is weight from recordings without a year).
    Only aggregated rows are collected.
    """
    labelled = nodes.filter(F.col("primary_genre").isNotNull()).select("artist_id")
    n = nodes.agg(
        F.count("*").alias("nodes"),
        F.mean(F.col("primary_genre").isNotNull().cast("double")).alias("labelled"),
        # eqNullSafe: unlabelled artists count as "not classical", not as missing.
        F.mean(F.col("primary_genre").eqNullSafe(build.CLASSICAL).cast("double")).alias(
            "classical"
        ),
    ).collect()[0]
    by_type = {
        r["edge_type"]: r
        for r in edges.groupBy("edge_type")
        .agg(F.count("*").alias("edges"), F.sum("weight").alias("weight"))
        .collect()
    }
    e = edges.agg(
        F.count("*").alias("edges"),
        F.sum("weight").alias("weight"),
        F.mean((F.col("weight") == 1).cast("double")).alias("weight1"),
    ).collect()[0]
    both = (
        edges.join(labelled.withColumnRenamed("artist_id", "src_id"), "src_id", "left_semi")
        .join(labelled.withColumnRenamed("artist_id", "dst_id"), "dst_id", "left_semi")
        .count()
    )
    metrics: dict[str, float] = {
        "nodes": n["nodes"],
        "edges": e["edges"],
        "total_weight": e["weight"],
    }
    for t in EDGE_TYPES:
        metrics[f"edges_{t}"] = by_type[t]["edges"] if t in by_type else 0
    for t in EDGE_TYPES:
        w = by_type[t]["weight"] if t in by_type else 0
        metrics[f"weight_share_{t}"] = round(w / e["weight"], 4)
    for t in EDGE_TYPES:
        metrics[f"edge_share_{t}"] = round(metrics[f"edges_{t}"] / e["edges"], 4)
    metrics.update(
        {
            "nodes_labelled_share": round(n["labelled"], 4),
            "nodes_classical_share": round(n["classical"], 4),
            "edges_labelled_share": round(both / e["edges"], 4),
            "edges_weight_1_share": round(e["weight1"], 4),
        }
    )
    metrics.update(degree_quantiles(edges))
    for r in (
        by_decade.groupBy("decade").agg(F.sum("weight").alias("w")).orderBy("decade").collect()
    ):
        metrics[f"weight_share_{r['decade']}s"] = round(r["w"] / e["weight"], 4)
    return metrics


def comparison_rows(full: dict[str, float], sample: dict[str, float]) -> list[dict]:
    """One row per metric: full value, sample value, and sample / full.

    Metrics present in only one build (e.g. a decade missing from the sample)
    get an empty cell on the other side; the ratio is empty when it is undefined.
    """
    rows = []
    for metric in list(full) + [m for m in sample if m not in full]:
        f, s = full.get(metric), sample.get(metric)
        ratio = round(s / f, 4) if f not in (None, 0) and s is not None else ""
        rows.append(
            {
                "metric": metric,
                "full": "" if f is None else f,
                "sample": "" if s is None else s,
                "sample_over_full": ratio,
            }
        )
    return rows


def run(cfg: Config, sample: float, accept_changes: bool) -> dict:
    """Run the chain and the checks for one fraction, then fingerprint and compare.

    Returns the manifest. A failing check (F3.3) stops before the manifest is written.
    """
    tag = sample_tag(sample)  # validates 0 < sample < 1
    clean = cfg.data_path("clean")
    missing = [t for t in FULL_ARTIST_TABLES if not (clean / t).is_dir()]
    if missing:
        raise ConfigError(
            f"full clean tables missing: {missing}; run musin.clean.artists and musin.clean.genres"
        )

    timings: dict[str, float] = {}
    for name, stage in (
        ("clean.credits", credits.run),
        ("clean.contributions", contributions.run),
        ("clean.years", years.run),
        ("graph.build", build.run),
        ("graph.checks", lambda c, s: checks.run(c, s, accept_changes)),
    ):
        start = time.monotonic()
        log.info("=== %s (sample %s)", name, sample)
        stage(cfg, sample)
        timings[name] = round(time.monotonic() - start, 1)

    out = layer_path(cfg, "graph", sample)
    full_graph = cfg.data_path("graph")
    repo_root = cfg.source.resolve().parent.parent
    spark = get_spark("musin-graph-sample")

    def read(root: Path, table: str) -> DataFrame:
        return spark.read.parquet(str(root / table))

    tables = {t: fingerprint(read(out, t)) for t in GRAPH_TABLES}
    manifest = {
        "sample_fraction": sample,
        "built_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo_commit": repo_commit(repo_root),
        "config": {k: cfg.get(k) for k in RESULT_SECTIONS},
        "stage_seconds": timings,
        "tables": tables,
    }
    (out / MANIFEST).write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    log.info("manifest -> %s", out / MANIFEST)
    for t, fp in tables.items():
        log.info("  %-16s %10d rows  fingerprint %s", t, fp["rows"], fp["fingerprint"])

    if all((full_graph / t).is_dir() for t in GRAPH_TABLES):
        profiles = [
            graph_profile(
                read(root, "nodes_artist"), read(root, "edges"), read(root, "edges_by_decade")
            )
            for root in (full_graph, out)
        ]
        rows = comparison_rows(*profiles)
        target = repo_root / cfg.get("reports_dir") / "tables" / f"sample_{tag}_vs_full.csv"
        write_table(target, rows)
        log.info("comparison -> %s", target)
        for r in rows:
            log.info(
                "  %-34s %14s %14s %8s", r["metric"], r["full"], r["sample"], r["sample_over_full"]
            )
    else:
        log.info("no full graph at %s: comparison skipped", full_graph)
    spark.stop()
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument(
        "--sample", type=float, required=True, help="fraction of recordings, e.g. 0.05"
    )
    parser.add_argument(
        "--accept-changes",
        action="store_true",
        help="passed to the graph checks: accept row-count changes beyond checks.max_row_change",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    run(load_config(args.config), args.sample, args.accept_changes)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
