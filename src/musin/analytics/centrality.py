"""Centrality measures and their agreement (F4.1, FR-15, RQ4).

    docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml

Reads:  the graph loaded in Neo4j ($MUSIN_DATA/neo4j/loaded_graph.json, F3.4)
Writes: :Artist properties degreeWeighted, pagerank, betweenness (COLLAB, PRD §8.3)
        <results>/centrality_top.csv        top-k artists per projection and measure
        <results>/centrality_agreement.csv  Spearman rho and top-k Jaccard per pair of
                                            measures, and per measure across projections
        <results>/centrality_runs.csv       parameters, runtimes, convergence per run
        <results> = reports/tables (full graph) or the sample tree (analytics.common)

For each projection (collab_undirected, then the credit_only sensitivity run,
PRD §5 RQ4) it builds the projection (F3.5), checks that projection plus the
largest algorithm estimate fit the heap, runs weighted degree, PageRank and
unweighted sampled betweenness in mutate mode with the PRD §5.5 settings,
streams the scores, checks them, writes the COLLAB scores back to Neo4j, and
drops the projection. PageRank must converge, and betweenness runs
single-threaded so that the same seed gives the same scores (D-033).
No Spark: the work happens in GDS, and the per-artist scores (≈ 1.2 M rows
on the full graph) come back from Neo4j as pandas frames. Exits 1 if a check
fails.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from itertools import combinations

import pandas as pd

from musin.analytics.common import CONNECT_WAIT_S, loaded_graph, results_dir, write_table
from musin.common.config import Config, load_config, require_env
from musin.common.neo4j import get_driver
from musin.gds.setup import Session, drop, parse_size, project

log = logging.getLogger("musin.analytics.centrality")

COLLAB = "collab_undirected"
CREDIT_ONLY = "credit_only"
# §8.3 property name -> algorithm statement in neo4j/04_centrality.cypher.
MEASURES: dict[str, str] = {
    "degreeWeighted": "degree",
    "pagerank": "pagerank",
    "betweenness": "betweenness",
}
CYPHER_FILES = ("02_projections", "04_centrality")
OUT_COLUMNS = {
    "artistId": "artist_id",
    "primaryGenre": "primary_genre",
    "degreeWeighted": "degree_weighted",
}


def top_k(scores: pd.DataFrame, measure: str, k: int) -> pd.DataFrame:
    """The k highest-scoring artists for one measure, with a 1-based `rank`.

    In: one row per artist with `artistId` and `measure`. Out: k rows of the
    same columns plus `rank`. Ties are broken by artistId ascending, so the
    list (and the top-k sets of agreement()) is deterministic.
    """
    ordered = scores.sort_values(
        [measure, "artistId"], ascending=[False, True], kind="mergesort"
    ).head(k)
    return ordered.assign(rank=range(1, len(ordered) + 1))


def jaccard(a: set, b: set) -> float:
    """|a ∩ b| / |a ∪ b|; 1.0 for two empty sets."""
    union = a | b
    return len(a & b) / len(union) if union else 1.0


def spearman(x: pd.Series, y: pd.Series) -> float:
    """Spearman rho: Pearson correlation of average ranks (ties share a rank)."""
    return float(x.rank(method="average").corr(y.rank(method="average")))


def agreement(scores: pd.DataFrame, measures: list[str], k: int) -> list[dict]:
    """Agreement of every pair of measures on one projection (PRD §5 RQ4).

    In: one row per artist with `artistId` and every measure. Out: one dict per
    pair with n_artists, spearman_rho over all artists, and the Jaccard overlap
    of the two top-k sets.
    """
    tops = {m: set(top_k(scores, m, k)["artistId"]) for m in measures}
    return [
        {
            "measure_a": a,
            "measure_b": b,
            "n_artists": len(scores),
            "spearman_rho": spearman(scores[a], scores[b]),
            "top_k": k,
            "jaccard_top_k": jaccard(tops[a], tops[b]),
        }
        for a, b in combinations(measures, 2)
    ]


def cross_projection(
    first: pd.DataFrame, second: pd.DataFrame, measures: list[str], k: int
) -> list[dict]:
    """Agreement of each measure between two projections (COLLAB vs credit_only).

    Spearman rho is taken over the artists present in both projections (inner
    join on artistId); the Jaccard overlap compares each projection's own
    top-k list, i.e. the lists a reader would see.
    """
    common = first.merge(second, on="artistId", suffixes=("_a", "_b"))
    rows = []
    for m in measures:
        rows.append(
            {
                "measure_a": m,
                "measure_b": m,
                "n_artists": len(common),
                "spearman_rho": spearman(common[f"{m}_a"], common[f"{m}_b"]),
                "top_k": k,
                "jaccard_top_k": jaccard(
                    set(top_k(first, m, k)["artistId"]), set(top_k(second, m, k)["artistId"])
                ),
            }
        )
    return rows


def run_projection(s: Session, name: str, params: dict, heap: int) -> tuple[pd.DataFrame, list]:
    """Build `name`, run the three measures, return (scores by nodeId, run rows).

    Raises RuntimeError if the estimate does not fit the heap or a score check
    fails. The projection is dropped again in every case.
    """
    drop(s.driver, name)
    built = project(s.driver, name)
    try:
        info = s.run("projections.graph_info", name=name)[0]
        estimates = {
            m: s.run(f"centrality.estimate_{alg}", graph=name, **params)[0]
            for m, alg in MEASURES.items()
        }
        worst = max(estimates, key=lambda m: estimates[m]["bytesMax"])
        need = info["sizeInBytes"] + estimates[worst]["bytesMax"]
        log.info(
            "%s: %d nodes, %d relationships, %s; largest estimate %s (%s)",
            name,
            built["nodes"],
            built["relationships"],
            info["memoryUsage"],
            estimates[worst]["requiredMemory"],
            worst,
        )
        if need >= heap:
            raise RuntimeError(
                f"{name}: {need / 2**30:.2f} GiB needed, heap {heap / 2**30:.0f} GiB"
            )

        runs = []
        for m, alg in MEASURES.items():
            t0 = time.monotonic()
            stats = s.run(f"centrality.{alg}", graph=name, **params)[0]
            seconds = time.monotonic() - t0
            log.info("  %-15s %6.1fs  %s", m, seconds, stats)
            if stats["didConverge"] is False:
                # D-033: max_iterations is a cap; an unconverged PageRank is not a result.
                raise RuntimeError(
                    f"{name}: {m} did not converge in {stats['ranIterations']} iterations;"
                    " raise analysis.pagerank.max_iterations (needs a decision entry)"
                )
            runs.append(
                {
                    "projection": name,
                    "measure": m,
                    "nodes": built["nodes"],
                    "relationships": built["relationships"],
                    "estimate_bytes_max": estimates[m]["bytesMax"],
                    "compute_millis": stats["computeMillis"],
                    "wall_seconds": round(seconds, 1),
                    "ran_iterations": stats["ranIterations"],
                    "did_converge": stats["didConverge"],
                }
            )

        scores = None
        for m in MEASURES:
            col = s.frame("centrality.stream_property", graph=name, property=m)
            col = col.rename(columns={"value": m})
            scores = col if scores is None else scores.merge(col, on="nodeId", how="outer")
        total = s.run("projections.weight_total", name=name)[0]["total"]
        problems = []
        if len(scores) != built["nodes"]:
            problems.append(f"{len(scores)} scored artists, {built['nodes']} projected")
        if scores[list(MEASURES)].isna().any().any():
            problems.append("missing scores")
        if scores["degreeWeighted"].sum() != total:
            problems.append(f"weighted degree sum {scores['degreeWeighted'].sum()} != {total}")
        if problems:
            raise RuntimeError(f"{name}: " + "; ".join(problems))
        log.info("  scores checked: %d artists, weighted degree sum %.0f", len(scores), total)

        if name == COLLAB:
            written = s.run(
                "centrality.write_back",
                graph=name,
                properties=list(MEASURES),
                concurrency=params["concurrency"],
            )[0]
            counts = s.run("centrality.count_written")[0]
            if any(counts[m] != counts["artists"] for m in MEASURES):
                raise RuntimeError(f"write-back incomplete: {counts}")
            log.info(
                "  wrote %d properties in %d ms",
                written["propertiesWritten"],
                written["writeMillis"],
            )
        return scores, runs
    finally:
        drop(s.driver, name)


def run(cfg: Config) -> dict[str, pd.DataFrame]:
    """Run F4.1 on the loaded graph and write its tables; return them by file stem."""
    loaded = loaded_graph(cfg)
    heap = parse_size(require_env("NEO4J_HEAP_SIZE"))
    params = {
        "damping": float(cfg.get("analysis.pagerank.damping")),
        "maxIterations": int(cfg.get("analysis.pagerank.max_iterations")),
        "samplingSize": int(cfg.get("analysis.betweenness_sampling_size")),
        "seed": int(cfg.get("analysis.random_seed")),
        "concurrency": int(cfg.get("analysis.concurrency")),
        "betweennessConcurrency": int(cfg.get("analysis.betweenness_concurrency")),
    }
    k_top = int(cfg.get("analysis.top_k_centrality"))
    k_overlap = int(cfg.get("analysis.top_k_overlap"))
    measures = list(MEASURES)
    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    log.info("graph in Neo4j: %s (edges fingerprint %s); %s", graph, fingerprint, params)

    driver = get_driver(CONNECT_WAIT_S)
    try:
        s = Session(driver, files=CYPHER_FILES)
        artists = s.frame("centrality.artists")
        scores, runs = {}, []
        for name in (COLLAB, CREDIT_ONLY):
            by_node, projection_runs = run_projection(s, name, params, heap)
            scores[name] = by_node.merge(artists, on="nodeId", how="left")
            runs += projection_runs
    finally:
        driver.close()

    top_rows, agree_rows = [], []
    for name, frame in scores.items():
        for m in measures:
            top_rows.append(top_k(frame, m, k_top).assign(projection=name, measure=m))
        agree_rows += [{"comparison": name, **r} for r in agreement(frame, measures, k_overlap)]
    agree_rows += [
        {"comparison": f"{COLLAB} vs {CREDIT_ONLY}", **r}
        for r in cross_projection(scores[COLLAB], scores[CREDIT_ONLY], measures, k_overlap)
    ]

    stamp = {"graph": graph, "edges_fingerprint": fingerprint}
    top = pd.concat(top_rows, ignore_index=True)[
        [
            "projection",
            "measure",
            "rank",
            "artistId",
            "mbid",
            "name",
            "type",
            "primaryGenre",
            *measures,
        ]
    ].rename(columns=OUT_COLUMNS)
    agree = pd.DataFrame(agree_rows)
    settings = {
        "concurrency": params["concurrency"],
        "betweenness_concurrency": params["betweennessConcurrency"],
        "damping": params["damping"],
        "max_iterations": params["maxIterations"],
        "sampling_size": params["samplingSize"],
        "sampling_seed": params["seed"],
    }
    runs_frame = pd.DataFrame(runs).assign(**settings)
    tables = {
        "centrality_top": top,
        "centrality_agreement": agree,
        "centrality_runs": runs_frame,
    }
    out = results_dir(cfg, graph)
    for stem, frame in tables.items():
        frame = frame.assign(**stamp)
        tables[stem] = frame
        log.info("%-22s %4d rows -> %s", stem, len(frame), write_table(frame, out / f"{stem}.csv"))
    for r in agree_rows:
        log.info(
            "  %-34s %-15s %-15s rho %.3f  jaccard@%d %.3f",
            r["comparison"],
            r["measure_a"],
            r["measure_b"],
            r["spearman_rho"],
            r["top_k"],
            r["jaccard_top_k"],
        )
    return tables


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # id(a) in neo4j/04_centrality.cypher raises a deprecation notice on every run.
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    start = time.monotonic()
    try:
        run(load_config(args.config))
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
