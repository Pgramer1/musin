"""PageRank on identical input in Neo4j GDS vs Spark GraphFrames (F5.2, FR-22).

    # 1. with Neo4j running (the loaded graph, loaded_graph.json):
    python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds
    # 2. with Neo4j stopped (docker compose stop neo4j), Spark gets the memory:
    python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine graphframes

Both engines compute **unweighted** PageRank on the COLLAB artist pairs of the
graph that Neo4j holds (D-042): GDS on the `collab_undirected` projection
(F3.5), GraphFrames on the same graph's Parquet (`source` in
loaded_graph.json), every pair in both directions. GraphFrames' PageRank has no
edge weights, so the weighted PageRank of F4.1 cannot be the common ground; it
is kept as a reference row. Both run to convergence with
`analysis.pagerank.damping`, `benchmark.pagerank.tolerance` and
`analysis.concurrency` cores, `benchmark.pagerank.repeats` times. Each
repetition is timed in three phases:

  load     GDS: build the projection from the store (Cypher aggregation)
           GraphFrames: read Parquet, form the pairs, cache and count
  compute  the PageRank call itself, until every score exists
  output   GDS: stream the scores to the client; GraphFrames: write Parquet

A GraphFrames repetition runs in a fresh Python + JVM process (as F5.1, D-041),
with the Spark event log on. Per-engine results go to
$MUSIN_DATA/<paths.benchmark>/pagerank/<graph>/: runs_<engine>.json and the
scores of the first repetition. When both engines have results for the same
graph fingerprint, either command writes the report:

  <results>/benchmark_pagerank_runs.csv       seconds per engine, repetition, phase
  <results>/benchmark_pagerank_summary.csv    medians per engine and phase
  <results>/benchmark_pagerank_agreement.csv  GDS vs GraphFrames (and each vs F4.1):
                                              Spearman rho, top-k Jaccard, score differences
  <results>/benchmark_pagerank_top.csv        top-k artists per engine

<results> is musin.common.layers.results_dir for the loaded graph.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from musin.analytics.centrality import jaccard, spearman, top_k
from musin.analytics.common import (
    CONNECT_WAIT_S,
    check_still_loaded,
    loaded_graph,
    results_dir,
    write_table,
)
from musin.common.config import Config, load_config
from musin.common.neo4j import get_driver

log = logging.getLogger("musin.benchmark.pagerank")

ENGINES = ("gds", "graphframes")
PHASES = ("load", "compute", "output")
COLLAB = "collab_undirected"
CYPHER_FILES = ("02_projections", "09_benchmark_pagerank")
REFERENCE = "f41_weighted"  # F4.1's weighted PageRank, the reference row


def settings(cfg: Config) -> dict:
    """The algorithm settings both engines share (D-042)."""
    return {
        "damping": float(cfg.get("analysis.pagerank.damping")),
        "max_iterations": int(cfg.get("analysis.pagerank.max_iterations")),
        "tolerance": float(cfg.get("benchmark.pagerank.tolerance")),
        "cores": int(cfg.get("analysis.concurrency")),
        "repeats": int(cfg.get("benchmark.pagerank.repeats")),
    }


def base_dir(cfg: Config, graph: str) -> Path:
    return cfg.data_path("benchmark") / "pagerank" / graph


def undirected_edges(edges):
    """GraphFrames edge list of the COLLAB pairs, both directions (D-017).

    In: edges(src_id, dst_id, ...) with any number of typed edges per pair.
    Out: (src, dst), one row per direction of each distinct unordered pair,
    the same pair set as the `collab_undirected` projection.
    """
    from pyspark.sql import functions as F

    pairs = edges.select(
        F.least("src_id", "dst_id").alias("a"), F.greatest("src_id", "dst_id").alias("b")
    ).distinct()
    return pairs.select(F.col("a").alias("src"), F.col("b").alias("dst")).unionByName(
        pairs.select(F.col("b").alias("src"), F.col("a").alias("dst"))
    )


def compare(a: pd.DataFrame, b: pd.DataFrame, k: int) -> dict:
    """Agreement of two score vectors over the same artists.

    In: frames with `artistId` and `score`. Out: n_artists, Spearman rho, the
    Jaccard overlap of the two top-k sets and the size of their intersection,
    and absolute / relative score differences (relative to `a`).
    """
    both = a.merge(b, on="artistId", suffixes=("_a", "_b"))
    if len(both) != len(a) or len(both) != len(b):
        raise RuntimeError(f"artist sets differ: {len(a)} vs {len(b)}, {len(both)} in common")
    top_a = set(top_k(a, "score", k)["artistId"])
    top_b = set(top_k(b, "score", k)["artistId"])
    diff = (both["score_a"] - both["score_b"]).abs()
    rel = diff / both["score_a"].abs()
    return {
        "n_artists": len(both),
        "spearman_rho": spearman(both["score_a"], both["score_b"]),
        "top_k": k,
        "jaccard_top_k": jaccard(top_a, top_b),
        "common_top_k": len(top_a & top_b),
        "max_abs_diff": float(diff.max()),
        "max_rel_diff": float(rel.max()),
        "median_rel_diff": float(rel.median()),
    }


def summary(runs: list[dict]) -> list[dict]:
    """Median seconds per engine and phase over the repetitions, plus totals."""
    rows = []
    for engine in ENGINES:
        mine = [r for r in runs if r["engine"] == engine]
        if not mine:
            continue
        row = {"engine": engine, "repeats": len(mine)}
        for phase in (*PHASES, "total"):
            values = [r[f"{phase}_s"] for r in mine]
            row[f"{phase}_median_s"] = round(statistics.median(values), 2)
            row[f"{phase}_min_s"] = round(min(values), 2)
            row[f"{phase}_max_s"] = round(max(values), 2)
        rows.append(row)
    if len(rows) == 2:
        gds, gf = rows
        for phase in (*PHASES, "total"):
            gf[f"{phase}_vs_gds"] = round(gf[f"{phase}_median_s"] / gds[f"{phase}_median_s"], 2)
    return rows


def _write_json(target: Path, data) -> None:
    """Write via a temp file, so an interrupted run leaves no partial result."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(target)


def _write_runs(base: Path, engine: str, runs: list[dict]) -> None:
    _write_json(base / f"runs_{engine}.json", runs)


def run_gds(cfg: Config, loaded: dict, opts: dict, base: Path) -> None:
    """All GDS repetitions in this process; scores of repetition 1 to Parquet."""
    from musin.gds.setup import Session, drop, project

    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    params = {
        "damping": opts["damping"],
        "maxIterations": opts["max_iterations"],
        "tolerance": opts["tolerance"],
        "concurrency": opts["cores"],
    }
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)  # id() deprecation
    driver = get_driver(CONNECT_WAIT_S)
    runs, scores = [], None
    try:
        s = Session(driver, files=CYPHER_FILES)
        for repeat in range(1, opts["repeats"] + 1):
            drop(driver, COLLAB)
            started = datetime.now(UTC)
            t0 = time.monotonic()
            built = project(driver, COLLAB)
            t1 = time.monotonic()
            stats = s.run("benchmark_pagerank.pagerank_unweighted", graph=COLLAB, **params)[0]
            t2 = time.monotonic()
            if not stats["didConverge"]:
                raise RuntimeError(f"GDS PageRank did not converge in {stats['ranIterations']}")
            frame = s.frame("benchmark_pagerank.stream_scores", graph=COLLAB)
            t3 = time.monotonic()
            drop(driver, COLLAB)
            run = {
                "engine": "gds",
                "repeat": repeat,
                "started_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "finished_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "load_s": round(t1 - t0, 3),
                "compute_s": round(t2 - t1, 3),
                "output_s": round(t3 - t2, 3),
                "total_s": round(t3 - t0, 3),
                "engine_compute_ms": stats["computeMillis"],
                "iterations": stats["ranIterations"],
                "nodes": built["nodes"],
                "directed_edges": built["relationships"],
                "score_sum": float(frame["score"].sum()),
                "graph": graph,
                "edges_fingerprint": fingerprint,
                **opts,
            }
            log.info(
                "gds r%d: load %.1fs compute %.1fs (%d iterations) output %.1fs",
                repeat, run["load_s"], run["compute_s"], run["iterations"], run["output_s"],
            )  # fmt: skip
            runs.append(run)
            if scores is None:
                scores = frame
        ids = s.frame("benchmark_pagerank.artist_ids")
    finally:
        drop(driver, COLLAB)
        driver.close()
    check_still_loaded(cfg, loaded)

    base.mkdir(parents=True, exist_ok=True)
    gds = scores.merge(ids, on="nodeId", how="left")[["artistId", "score"]]
    gds.to_parquet(base / "scores_gds.parquet", index=False)
    ids[["artistId", "mbid", "name"]].to_parquet(base / "artists.parquet", index=False)
    ref = ids[["artistId", "pagerankWeighted"]].rename(columns={"pagerankWeighted": "score"})
    if ref["score"].notna().all():
        ref.to_parquet(base / f"scores_{REFERENCE}.parquet", index=False)
    else:
        log.info("F4.1 pagerank missing on %d artists: no reference", ref["score"].isna().sum())
    _write_runs(base, "gds", runs)


def run_graphframes_once(loaded: dict, opts: dict, base: Path, repeat: int) -> dict:
    """One GraphFrames repetition in this (fresh) process; returns its run row."""
    from graphframes import GraphFrame
    from pyspark.sql import functions as F

    from musin.common.spark import get_spark, use_benchmark_settings

    use_benchmark_settings(opts["cores"], base / "spark-events")
    spark = get_spark("musin-benchmark-pagerank")
    source = Path(loaded["source"])
    started = datetime.now(UTC)
    t0 = time.monotonic()
    vertices = (
        spark.read.parquet(str(source / "nodes_artist"))
        .select(F.col("artist_id").alias("id"))
        .cache()
    )
    edges = undirected_edges(spark.read.parquet(str(source / "edges"))).cache()
    nodes, directed = vertices.count(), edges.count()
    t1 = time.monotonic()
    ranked = (
        GraphFrame(vertices, edges)
        .pageRank(resetProbability=1 - opts["damping"], tol=opts["tolerance"])
        .vertices.select(F.col("id").alias("artistId"), F.col("pagerank").alias("score"))
        .cache()
    )
    ranked.count()
    t2 = time.monotonic()
    ranked.write.mode("overwrite").parquet(str(base / f"scores_graphframes_r{repeat}"))
    t3 = time.monotonic()
    score_sum = ranked.agg(F.sum("score")).collect()[0][0]
    spark.stop()
    return {
        "engine": "graphframes",
        "repeat": repeat,
        "started_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "load_s": round(t1 - t0, 3),
        "compute_s": round(t2 - t1, 3),
        "output_s": round(t3 - t2, 3),
        "total_s": round(t3 - t0, 3),
        "engine_compute_ms": None,
        "iterations": None,  # GraphFrames' tolerance mode does not report iterations
        "nodes": nodes,
        "directed_edges": directed,
        "score_sum": float(score_sum),
        "graph": loaded["graph"],
        "edges_fingerprint": loaded["graph_fingerprints"]["edges"],
        **opts,
    }


def run_graphframes(config_path: str, opts: dict, base: Path) -> None:
    """Every GraphFrames repetition in its own process; scores of repetition 1 kept."""
    runs = []
    for repeat in range(1, opts["repeats"] + 1):
        out = base / f"run_graphframes_r{repeat}.json"
        cmd = [
            sys.executable, "-m", "musin.benchmark.pagerank", "--config", config_path,
            "--engine", "graphframes", "--repeat", str(repeat),
        ]  # fmt: skip
        subprocess.run(cmd, check=True)
        run = json.loads(out.read_text())
        log.info(
            "graphframes r%d: load %.1fs compute %.1fs output %.1fs",
            repeat, run["load_s"], run["compute_s"], run["output_s"],
        )  # fmt: skip
        runs.append(run)
    _write_runs(base, "graphframes", runs)


def _read_scores(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)[["artistId", "score"]]


def report(cfg: Config, loaded: dict, base: Path) -> bool:
    """Write the four report tables if both engines have results for this graph."""
    fingerprint = loaded["graph_fingerprints"]["edges"]
    runs = []
    for engine in ENGINES:
        path = base / f"runs_{engine}.json"
        if not path.is_file():
            log.info("no %s results yet in %s: report not written", engine, base)
            return False
        rows = json.loads(path.read_text())
        if any(r["edges_fingerprint"] != fingerprint for r in rows):
            raise RuntimeError(f"{path} is from another graph build; re-run --engine {engine}")
        runs += rows
    gds_run = next(r for r in runs if r["engine"] == "gds")
    for r in runs:
        if (r["nodes"], r["directed_edges"]) != (gds_run["nodes"], gds_run["directed_edges"]):
            raise RuntimeError(
                f"input differs: {r['engine']} r{r['repeat']} has {r['nodes']} nodes and "
                f"{r['directed_edges']} directed edges, GDS {gds_run['nodes']} and "
                f"{gds_run['directed_edges']}"
            )

    k = int(cfg.get("analysis.top_k_overlap"))
    k_list = int(cfg.get("analysis.top_k_centrality"))
    scores = {
        "gds": _read_scores(base / "scores_gds.parquet"),
        "graphframes": _read_scores(base / "scores_graphframes_r1"),
    }
    reference = base / f"scores_{REFERENCE}.parquet"
    pairs = [("gds", "graphframes")]
    if reference.is_file():
        scores[REFERENCE] = _read_scores(reference)
        pairs += [(REFERENCE, "gds"), (REFERENCE, "graphframes")]
    agreement = [{"a": a, "b": b, **compare(scores[a], scores[b], k)} for a, b in pairs]

    artists = pd.read_parquet(base / "artists.parquet")
    tops = []
    for engine, frame in scores.items():
        top = top_k(frame, "score", k_list).merge(artists, on="artistId", how="left")
        tops.append(
            top.assign(engine=engine)[["engine", "rank", "artistId", "mbid", "name", "score"]]
        )

    stamp = {"graph": loaded["graph"], "edges_fingerprint": fingerprint}
    tables = {
        "benchmark_pagerank_runs": pd.DataFrame(runs).drop(columns=["graph", "edges_fingerprint"]),
        "benchmark_pagerank_summary": pd.DataFrame(summary(runs)),
        "benchmark_pagerank_agreement": pd.DataFrame(agreement),
        "benchmark_pagerank_top": pd.concat(tops, ignore_index=True),
    }
    out = results_dir(cfg, loaded["graph"])
    for stem, frame in tables.items():
        path = write_table(frame.assign(**stamp), out / f"{stem}.csv")
        log.info("%-30s %4d rows -> %s", stem, len(frame), path)
    for row in summary(runs):
        log.info(
            "  %-12s load %7.1fs  compute %7.1fs  output %6.1fs  total %7.1fs (median of %d)",
            row["engine"], row["load_median_s"], row["compute_median_s"],
            row["output_median_s"], row["total_median_s"], row["repeats"],
        )  # fmt: skip
    for r in agreement:
        log.info(
            "  %-12s vs %-12s rho %.6f  jaccard@%d %.3f  max rel diff %.2e",
            r["a"], r["b"], r["spearman_rho"], r["top_k"], r["jaccard_top_k"], r["max_rel_diff"],
        )  # fmt: skip
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--engine", required=True, choices=ENGINES)
    parser.add_argument(
        "--repeat", type=int, help=argparse.SUPPRESS
    )  # internal: one GraphFrames run
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    loaded = loaded_graph(cfg)
    opts = settings(cfg)
    base = base_dir(cfg, loaded["graph"])

    if args.repeat is not None:
        run = run_graphframes_once(loaded, opts, base, args.repeat)
        _write_json(base / f"run_graphframes_r{args.repeat}.json", run)  # read by the parent
        return 0

    start = time.monotonic()
    log.info(
        "graph %s (edges fingerprint %s); engine %s; %s",
        loaded["graph"], loaded["graph_fingerprints"]["edges"], args.engine, opts,
    )  # fmt: skip
    if args.engine == "gds":
        run_gds(cfg, loaded, opts, base)
    else:
        run_graphframes(args.config, opts, base)
    report(cfg, loaded, base)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
