"""PageRank on identical input in Neo4j GDS, Spark GraphFrames and RAPIDS cuGraph (F5.2, F5.3).

    # 1. with Neo4j running (the loaded graph, loaded_graph.json):
    python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds
    # 2. with Neo4j stopped (docker compose stop neo4j), Spark gets the memory:
    python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine graphframes
    # 3. optional, on the GPU (F5.3): musin.benchmark.pagerank_gpu in the rapids container,
    #    then rewrite the report with all three engines:
    python -m musin.benchmark.pagerank --config config/pipeline.yaml --report

Every engine computes **unweighted** PageRank on the COLLAB artist pairs of the
graph that Neo4j holds (D-042): GDS on the `collab_undirected` projection
(F3.5), GraphFrames and cuGraph on the same graph's Parquet (`source` in
loaded_graph.json). GraphFrames' PageRank has no edge weights, so the weighted
PageRank of F4.1 cannot be the common ground; it is kept as a reference row.
All run to convergence with `analysis.pagerank.damping` and
`benchmark.pagerank.tolerance`; GDS and GraphFrames on `analysis.concurrency`
cores, cuGraph on the GPU (D-043). Each engine runs `benchmark.pagerank.repeats`
times, and each repetition is timed in three phases:

  load     GDS: build the projection from the store (Cypher aggregation)
           GraphFrames / cuGraph: read Parquet, form the pairs, build the graph
  compute  the PageRank call itself, until every score exists
  output   GDS: stream the scores to the client; the others: write Parquet

A GraphFrames or cuGraph repetition runs in a fresh process (as F5.1, D-041).
Every finished repetition is saved at once to
$MUSIN_DATA/<paths.benchmark>/pagerank/<graph>/run_<engine>_r<n>.json, and a
re-run skips repetitions already saved for the same graph fingerprint (--fresh
discards them). When GDS and GraphFrames are complete, each command writes the
report (cuGraph is included when its repetitions are complete too):

  <results>/benchmark_pagerank_runs.csv       seconds per engine, repetition, phase
  <results>/benchmark_pagerank_summary.csv    medians per engine and phase, ratio to GDS
  <results>/benchmark_pagerank_agreement.csv  every engine pair and each vs F4.1:
                                              Spearman rho, top-k Jaccard, score differences
  <results>/benchmark_pagerank_top.csv        top-k artists per engine

<results> is musin.common.layers.results_dir for the loaded graph.
"""

from __future__ import annotations

import argparse
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
from musin.benchmark.common import (
    PHASES,
    TIMESTAMP,
    base_dir,
    collect,
    finished,
    run_file,
    settings,
    write_json,
)
from musin.common.config import Config, load_config
from musin.common.neo4j import get_driver

log = logging.getLogger("musin.benchmark.pagerank")

ENGINES = ("gds", "graphframes", "cugraph")
REQUIRED = ("gds", "graphframes")  # FR-22; cuGraph (FR-23) is optional
COLLAB = "collab_undirected"
CYPHER_FILES = ("02_projections", "09_benchmark_pagerank")
REFERENCE = "f41_weighted"  # F4.1's weighted PageRank, the reference row
# Where each engine's scores of repetition 1 are stored (relative to base_dir).
SCORES = {
    "gds": "scores_gds.parquet",
    "graphframes": "scores_graphframes_r1",
    "cugraph": "scores_cugraph_r1.parquet",
    REFERENCE: f"scores_{REFERENCE}.parquet",
}


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
    """Median seconds per engine and phase over the repetitions, plus ratios to GDS."""
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
    gds = next((r for r in rows if r["engine"] == "gds"), None)
    if gds is not None:
        for row in rows:
            if row is not gds:
                for phase in (*PHASES, "total"):
                    row[f"{phase}_vs_gds"] = round(
                        row[f"{phase}_median_s"] / gds[f"{phase}_median_s"], 2
                    )
    return rows


def run_gds(cfg: Config, loaded: dict, opts: dict, base: Path) -> None:
    """Unfinished GDS repetitions in this process; scores of repetition 1 to Parquet."""
    from musin.gds.setup import Session, drop, project

    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    todo = [r for r in range(1, opts["repeats"] + 1) if not finished(base, "gds", r, fingerprint)]
    if not todo:
        log.info("gds: all %d repetitions already saved", opts["repeats"])
        return
    params = {
        "damping": opts["damping"],
        "maxIterations": opts["max_iterations"],
        "tolerance": opts["tolerance"],
        "concurrency": opts["cores"],
    }
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)  # id() deprecation
    driver = get_driver(CONNECT_WAIT_S)
    try:
        s = Session(driver, files=CYPHER_FILES)
        ids = s.frame("benchmark_pagerank.artist_ids")
        for repeat in todo:
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
            check_still_loaded(cfg, loaded)
            if repeat == 1:
                base.mkdir(parents=True, exist_ok=True)
                scores = frame.merge(ids, on="nodeId", how="left")[["artistId", "score"]]
                scores.to_parquet(base / SCORES["gds"], index=False)
                ids[["artistId", "mbid", "name"]].to_parquet(base / "artists.parquet", index=False)
                ref = ids[["artistId", "pagerankWeighted"]].rename(
                    columns={"pagerankWeighted": "score"}
                )
                if ref["score"].notna().all():
                    ref.to_parquet(base / SCORES[REFERENCE], index=False)
                else:
                    log.info("F4.1 pagerank missing on %d artists", ref["score"].isna().sum())
            run = {
                "engine": "gds",
                "repeat": repeat,
                "started_at_utc": started.strftime(TIMESTAMP),
                "finished_at_utc": datetime.now(UTC).strftime(TIMESTAMP),
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
            write_json(run_file(base, "gds", repeat), run)
            log.info(
                "gds r%d: load %.1fs compute %.1fs (%d iterations) output %.1fs",
                repeat, run["load_s"], run["compute_s"], run["iterations"], run["output_s"],
            )  # fmt: skip
    finally:
        drop(driver, COLLAB)
        driver.close()


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
    finished_at = datetime.now(UTC)
    score_sum = ranked.agg(F.sum("score")).collect()[0][0]
    spark.stop()
    return {
        "engine": "graphframes",
        "repeat": repeat,
        "started_at_utc": started.strftime(TIMESTAMP),
        "finished_at_utc": finished_at.strftime(TIMESTAMP),
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


def run_graphframes(config_path: str, loaded: dict, opts: dict, base: Path) -> None:
    """Every unfinished GraphFrames repetition in its own process."""
    fingerprint = loaded["graph_fingerprints"]["edges"]
    for repeat in range(1, opts["repeats"] + 1):
        if finished(base, "graphframes", repeat, fingerprint):
            log.info("graphframes r%d already saved, skipped", repeat)
            continue
        cmd = [
            sys.executable, "-m", "musin.benchmark.pagerank", "--config", config_path,
            "--engine", "graphframes", "--repeat", str(repeat),
        ]  # fmt: skip
        subprocess.run(cmd, check=True)
        run = finished(base, "graphframes", repeat, fingerprint)
        log.info(
            "graphframes r%d: load %.1fs compute %.1fs output %.1fs",
            repeat, run["load_s"], run["compute_s"], run["output_s"],
        )  # fmt: skip


def _read_scores(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)[["artistId", "score"]]


def report(cfg: Config, loaded: dict, base: Path) -> bool:
    """Write the report tables if GDS and GraphFrames are complete for this graph."""
    fingerprint = loaded["graph_fingerprints"]["edges"]
    repeats = int(cfg.get("benchmark.pagerank.repeats"))
    by_engine = {e: collect(base, e, repeats, fingerprint) for e in ENGINES}
    missing = [e for e in REQUIRED if by_engine[e] is None]
    if missing:
        log.info("%s not complete for this graph in %s: report not written", missing, base)
        return False
    engines = [e for e in ENGINES if by_engine[e] is not None]
    runs = [r for e in engines for r in by_engine[e]]
    gds_run = by_engine["gds"][0]
    for r in runs:
        if (r["nodes"], r["directed_edges"]) != (gds_run["nodes"], gds_run["directed_edges"]):
            raise RuntimeError(
                f"input differs: {r['engine']} r{r['repeat']} has {r['nodes']} nodes and "
                f"{r['directed_edges']} directed edges, GDS {gds_run['nodes']} and "
                f"{gds_run['directed_edges']}"
            )

    k = int(cfg.get("analysis.top_k_overlap"))
    k_list = int(cfg.get("analysis.top_k_centrality"))
    scores = {e: _read_scores(base / SCORES[e]) for e in engines}
    pairs = [(a, b) for i, a in enumerate(engines) for b in engines[i + 1 :]]
    if (base / SCORES[REFERENCE]).is_file():
        scores[REFERENCE] = _read_scores(base / SCORES[REFERENCE])
        pairs += [(REFERENCE, e) for e in engines]
    agreement = [{"a": a, "b": b, **compare(scores[a], scores[b], k)} for a, b in pairs]

    artists = pd.read_parquet(base / "artists.parquet")
    tops = []
    for engine, frame in scores.items():
        top = top_k(frame, "score", k_list).merge(artists, on="artistId", how="left")
        tops.append(
            top.assign(engine=engine)[["engine", "rank", "artistId", "mbid", "name", "score"]]
        )

    stamp = {"graph": loaded["graph"], "edges_fingerprint": fingerprint}
    stats = summary(runs)
    tables = {
        "benchmark_pagerank_runs": pd.DataFrame(runs).drop(columns=["graph", "edges_fingerprint"]),
        "benchmark_pagerank_summary": pd.DataFrame(stats),
        "benchmark_pagerank_agreement": pd.DataFrame(agreement),
        "benchmark_pagerank_top": pd.concat(tops, ignore_index=True),
    }
    out = results_dir(cfg, loaded["graph"])
    for stem, frame in tables.items():
        path = write_table(frame.assign(**stamp), out / f"{stem}.csv")
        log.info("%-30s %4d rows -> %s", stem, len(frame), path)
    for row in stats:
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
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--engine", choices=("gds", "graphframes"))
    mode.add_argument("--report", action="store_true", help="only rewrite the report tables")
    parser.add_argument("--fresh", action="store_true", help="discard this engine's saved runs")
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
        write_json(run_file(base, "graphframes", args.repeat), run)  # read by the parent
        return 0

    start = time.monotonic()
    log.info(
        "graph %s (edges fingerprint %s); %s; %s",
        loaded["graph"], loaded["graph_fingerprints"]["edges"],
        "report" if args.report else f"engine {args.engine}", opts,
    )  # fmt: skip
    if args.fresh and args.engine:
        for path in base.glob(f"run_{args.engine}_r*.json"):
            path.unlink()
        log.info("--fresh: discarded saved %s runs", args.engine)
    if args.engine == "gds":
        run_gds(cfg, loaded, opts, base)
    elif args.engine == "graphframes":
        run_graphframes(args.config, loaded, opts, base)
    report(cfg, loaded, base)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
