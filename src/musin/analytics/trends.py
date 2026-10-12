"""Cross-genre and featuring share by decade (F4.4, FR-18, RQ3).

    docker compose exec spark python -m musin.analytics.trends --config config/pipeline.yaml \
        [--sample 0.05] [--variant <name>]

Reads:  <graph>/edges_by_decade, <graph>/nodes_artist  (F3.1; sample/variant tree per flags)
Writes: <results>/trends_by_decade.csv          one row per decade in analysis.decades
        <figures>/trends_by_decade.png           both shares as lines over the decades
        <results> = reports/tables (full graph), else the sample/variant tree
        (musin.common.layers.results_dir); <figures> from musin.common.layers.figures_dir

A Spark stage on the Parquet graph layer, not on Neo4j: edges_by_decade
(PRD §8.1) already splits every edge's weight by the decade of each shared
recording's first release year, which the Neo4j graph does not hold. For each
decade from analysis.decades.from to analysis.decades.to (PRD §5 RQ3):

    cross-genre share = weight of labelled edges whose endpoints have different
                        primary genres / weight of all labelled edges
    featuring share   = FEATURED weight / (FEATURED + CO_CREDITED) weight, all edges

An edge is labelled when both endpoints have a primary genre (PRD §5). Only
the per-decade aggregate (a handful of rows) leaves Spark.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.analytics.common import write_table
from musin.common.config import Config, ConfigError, load_config
from musin.common.layers import figures_dir, graph_name, layer_path, results_dir
from musin.common.spark import get_spark
from musin.graph.build import CO_CREDITED, EDGE_TYPES, FEATURED

log = logging.getLogger("musin.analytics.trends")

TYPE_COLUMNS = {t: f"{t.lower()}_weight" for t in EDGE_TYPES}


def decade_shares(by_decade: DataFrame, nodes: DataFrame, first: int, last: int) -> DataFrame:
    """Per-decade weights and the two RQ3 shares, for decades first..last.

    In: edges_by_decade (src_id, dst_id, edge_type, decade, weight) and
    nodes_artist (artist_id, primary_genre). Out: one row per decade that has
    edges, with edges (rows), total_weight, labelled_weight, labelled_share,
    cross_weight, cross_genre_share, <type>_weight for every edge type, and
    featuring_share. A share is null when its denominator is 0.
    """
    genre = nodes.select("artist_id", "primary_genre")
    src = genre.select(F.col("artist_id").alias("src_id"), F.col("primary_genre").alias("g_src"))
    dst = genre.select(F.col("artist_id").alias("dst_id"), F.col("primary_genre").alias("g_dst"))
    rows = (
        by_decade.filter(F.col("decade").between(first, last))
        .join(src, "src_id", "left")
        .join(dst, "dst_id", "left")
    )
    labelled = F.col("g_src").isNotNull() & F.col("g_dst").isNotNull()
    cross = labelled & (F.col("g_src") != F.col("g_dst"))

    def weight_if(cond):
        return F.sum(F.when(cond, F.col("weight")).otherwise(F.lit(0)))

    agg = rows.groupBy("decade").agg(
        F.count("*").alias("edges"),
        F.sum("weight").alias("total_weight"),
        weight_if(labelled).alias("labelled_weight"),
        weight_if(cross).alias("cross_weight"),
        *[weight_if(F.col("edge_type") == t).alias(c) for t, c in TYPE_COLUMNS.items()],
    )

    def share(num, den):
        return F.when(F.col(den) > 0, F.col(num) / F.col(den))

    featured, co_credited = TYPE_COLUMNS[FEATURED], TYPE_COLUMNS[CO_CREDITED]
    return (
        agg.withColumn("labelled_share", share("labelled_weight", "total_weight"))
        .withColumn("cross_genre_share", share("cross_weight", "labelled_weight"))
        .withColumn("_feat_base", F.col(featured) + F.col(co_credited))
        .withColumn("featuring_share", share(featured, "_feat_base"))
        .drop("_feat_base")
        .orderBy("decade")
    )


def complete_decades(frame: pd.DataFrame, first: int, last: int) -> pd.DataFrame:
    """Add a zero row for every decade in first..last that has no edges.

    In/out: the collected decade_shares() frame; out has one row per decade,
    ordered, with weights 0 and shares null (NaN) where a decade is empty.
    """
    decades = pd.DataFrame({"decade": range(first, last + 1, 10)})
    out = decades.merge(frame, on="decade", how="left")
    weights = ["edges", "total_weight", "labelled_weight", "cross_weight", *TYPE_COLUMNS.values()]
    out[weights] = out[weights].fillna(0).astype("int64")
    return out


def plot(frame: pd.DataFrame, target: Path, graph: str) -> Path:
    """Line chart of cross-genre and featuring share by decade (PNG)."""
    from musin.profiling.figures import DPI, SERIES, TEXT_2, _style, plt

    _style()
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    labels = [f"{d}s" for d in frame["decade"]]
    lines = [
        ("cross_genre_share", "Cross-genre share (labelled edges)"),
        ("featuring_share", "Featuring share (FEATURED / FEATURED + CO_CREDITED)"),
    ]
    for (column, label), color in zip(lines, SERIES, strict=True):
        ax.plot(
            labels,
            frame[column] * 100,
            color=color,
            linewidth=2,
            marker="o",
            markersize=5,
            label=label,
        )
        last = frame[column].last_valid_index()
        if last is not None:
            ax.annotate(
                f"{frame[column][last]:.0%}",
                (labels[last], frame[column][last] * 100),
                xytext=(6, 0),
                textcoords="offset points",
                va="center",
                color=TEXT_2,
                fontsize=8,
            )
    ax.set_ylim(0, max(100 * frame[[c for c, _ in lines]].max().max() * 1.15, 1))
    ax.yaxis.set_major_formatter(lambda y, _pos: f"{y:.0f}%")
    ax.set_title("Cross-genre and featuring share of collaboration weight, by decade")
    ax.set_xlabel(f"decade of first release · graph {graph}")
    ax.legend(loc="upper left", fontsize=8)
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(target, dpi=DPI)
    plt.close(fig)
    return target


def run(cfg: Config, sample: float | None) -> pd.DataFrame:
    """Run F4.4 on the graph layer chosen by `sample` and cfg.variant; return the table."""
    first, last = int(cfg.get("analysis.decades.from")), int(cfg.get("analysis.decades.to"))
    if first % 10 or last % 10 or first > last:
        raise ConfigError(
            f"analysis.decades must be decade starts with from <= to: {first}, {last}"
        )
    graph = graph_name(sample, cfg.variant)
    root = layer_path(cfg, "graph", sample)
    log.info("graph %s: %s; decades %ds-%ds", graph, root, first, last)

    spark = get_spark("musin-analytics-trends")
    by_decade = spark.read.parquet(str(root / "edges_by_decade"))
    nodes = spark.read.parquet(str(root / "nodes_artist"))
    totals = by_decade.agg(F.count("*").alias("rows"), F.sum("weight").alias("w")).collect()[0]
    frame = decade_shares(by_decade, nodes, first, last).toPandas()  # ≤ one row per decade
    spark.stop()

    frame = complete_decades(frame, first, last)
    inside = int(frame["total_weight"].sum())
    log.info(
        "edges_by_decade: %d rows, weight %d; %d (%.1f %%) inside %ds-%ds",
        totals["rows"],
        totals["w"],
        inside,
        100 * inside / totals["w"],
        first,
        last,
    )
    frame = frame.assign(graph=graph)
    out = results_dir(cfg, graph)
    log.info(
        "trends_by_decade %d rows -> %s",
        len(frame),
        write_table(frame, out / "trends_by_decade.csv"),
    )
    log.info("figure -> %s", plot(frame, figures_dir(cfg, graph) / "trends_by_decade.png", graph))
    for r in frame.itertuples():
        log.info(
            "  %ds  weight %9d  labelled %5.1f %%  cross-genre %5.1f %%  featuring %5.1f %%",
            r.decade,
            r.total_weight,
            100 * (r.labelled_share or 0),
            100 * (r.cross_genre_share or 0),
            100 * (r.featuring_share or 0),
        )
    return frame


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--sample", type=float, help="use the sample graph of this fraction")
    parser.add_argument("--variant", help="use the variant graph defined under variants.<name>")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    try:
        run(load_config(args.config, args.variant), args.sample)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
