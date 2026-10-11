"""Constraints, indexes, and GDS projections (F3.5, FR-14).

    docker compose exec spark python -m musin.gds.setup --config config/pipeline.yaml \
        [--keep-projections]

Run by neo4j/import_graph.sh after every verified import (F3.4).
Reads:  $MUSIN_DATA/neo4j/loaded_graph.json  (which graph Neo4j holds, F3.4)
        the Parquet edges of that graph (expected values), the Neo4j database
Writes: constraints and indexes in Neo4j (neo4j/01_constraints.cypher)
        <reports_dir>/tables/gds_projections.csv  (only when the full graph is loaded)

For each projection in PROJECTIONS (neo4j/02_projections.cypher) it estimates
the heap first, builds it, checks it against the Parquet graph (node count,
one relationship per pair in each direction, total weight, heaviest pairs),
estimates the PRD §5.5 algorithms on it with the `analysis.*` settings
(neo4j/03_estimates.cypher), checks that projection plus the largest
algorithm estimate fit the Neo4j heap, and drops it again (RULES §7) unless
--keep-projections. P4 code builds projections with project() and drop().
Exits 1 if a check fails.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from pathlib import Path

from neo4j import Driver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.common.config import Config, ConfigError, load_config, require_env
from musin.common.cypher import load_queries
from musin.common.neo4j import get_driver
from musin.common.spark import get_spark
from musin.export.neo4j_check import LOADED_MARKER
from musin.graph.build import CO_CREDITED, EDGE_TYPES, FEATURED

log = logging.getLogger("musin.gds.setup")

CYPHER_DIR = Path(__file__).resolve().parents[3] / "neo4j"
CONNECT_WAIT_S = 180
# The named projections of PRD §5 / RULES §7 and the edge types each merges.
PROJECTIONS: dict[str, list[str]] = {
    "collab_undirected": list(EDGE_TYPES),
    "credit_only": [CO_CREDITED, FEATURED],
}
CONSTRAINTS = ("artist_id_unique", "artist_mbid_unique", "genre_name_unique")
INDEXES = ("artist_name",)
ALGORITHMS = ("degree", "pagerank", "betweenness", "louvain")
TOP_PAIRS = 5  # heaviest pairs compared with Parquet

_SIZE = re.compile(r"^\s*(\d+)\s*([kmgt]?)b?\s*$", re.IGNORECASE)


def parse_size(text: str) -> int:
    """Neo4j memory setting ('8G', '512m', '1024') in bytes."""
    m = _SIZE.match(text)
    if not m:
        raise ConfigError(f"cannot parse memory size {text!r}")
    return int(m.group(1)) * 1024 ** "_kmgt".index((m.group(2) or "_").lower())


def pair_expectations(edges: DataFrame, types: list[str], k: int) -> dict:
    """What a pair projection of `types` must contain, computed from Parquet.

    In: §8.1 edges. Out: {"nodes", "pairs", "total_weight", "top": [(a, b, w)]}:
    artists with an edge of `types`, unordered pairs, summed weight, and the
    `k` heaviest pairs (ties by a, b), as in neo4j/02_projections.cypher.
    """
    pairs = (
        edges.filter(F.col("edge_type").isin(types))
        .groupBy(F.least("src_id", "dst_id").alias("a"), F.greatest("src_id", "dst_id").alias("b"))
        .agg(F.sum("weight").alias("w"))
        .cache()
    )
    stats = pairs.agg(F.count("*").alias("pairs"), F.sum("w").alias("w")).collect()[0]
    nodes = (
        pairs.select(F.col("a").alias("id"))
        .unionByName(pairs.select(F.col("b").alias("id")))
        .distinct()
        .count()
    )
    top = [
        (r["a"], r["b"], r["w"]) for r in pairs.orderBy(F.desc("w"), "a", "b").limit(k).collect()
    ]
    pairs.unpersist()
    return {"nodes": nodes, "pairs": stats["pairs"], "total_weight": stats["w"] or 0, "top": top}


def native_projection(types: list[str]) -> dict:
    """Relationship projection map for gds.graph.project.estimate."""
    return {t: {"type": t, "orientation": "UNDIRECTED", "properties": "weight"} for t in types}


class Session:
    """Named Cypher statements from neo4j/0*.cypher on one driver."""

    def __init__(self, driver: Driver) -> None:
        self.driver = driver
        self.queries: dict[str, str] = {}
        for f in ("01_constraints", "02_projections", "03_estimates"):
            for name, text in load_queries(CYPHER_DIR / f"{f}.cypher").items():
                self.queries[f"{f[3:]}.{name}"] = text

    def run(self, statement: str, /, **params) -> list[dict]:
        """Run `<file>.<name>` with Cypher parameters (one of them may be called `name`)."""
        query = self.queries[statement]
        records, _, _ = self.driver.execute_query(query, params, database_="neo4j")
        return [r.data() for r in records]


def project(driver: Driver, name: str) -> dict:
    """Build projection `name` (PROJECTIONS) and return its graph/nodes/relationships/millis."""
    if name not in PROJECTIONS:
        raise ValueError(f"unknown projection {name!r}; known: {sorted(PROJECTIONS)}")
    return Session(driver).run("projections.project_pairs", name=name, types=PROJECTIONS[name])[0]


def drop(driver: Driver, name: str) -> None:
    """Drop projection `name` if it exists."""
    Session(driver).run("projections.drop", name=name)


def run(cfg: Config, keep: bool) -> list[dict]:
    """Apply constraints, build and check every projection; raise RuntimeError on failure."""
    marker_file = cfg.data_root / "neo4j" / LOADED_MARKER
    if not marker_file.is_file():
        raise ConfigError(
            f"{marker_file} missing: Neo4j content unknown; run neo4j/import_graph.sh"
        )
    loaded = json.loads(marker_file.read_text())
    if loaded["graph"] == "toy":
        raise ConfigError("Neo4j holds the F1.5 toy graph; import a real graph first")
    heap = parse_size(require_env("NEO4J_HEAP_SIZE"))
    analysis = {
        "damping": float(cfg.get("analysis.pagerank.damping")),
        "maxIterations": int(cfg.get("analysis.pagerank.max_iterations")),
        "samplingSize": int(cfg.get("analysis.betweenness_sampling_size")),
        "seed": int(cfg.get("analysis.random_seed")),
    }
    problems: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        (log.info if ok else log.error)("%s %-40s %s", "PASS" if ok else "FAIL", name, detail)
        if not ok:
            problems.append(f"{name} {detail}".strip())

    # Expected values first, then Spark stops before GDS uses its heap (PRD §7.2).
    spark = get_spark("musin-gds-setup")
    edges = spark.read.parquet(str(Path(loaded["source"]) / "edges"))
    expected = {n: pair_expectations(edges, t, TOP_PAIRS) for n, t in PROJECTIONS.items()}
    spark.stop()

    driver = get_driver(CONNECT_WAIT_S)
    s = Session(driver)
    log.info(
        "graph in Neo4j: %s (edges fingerprint %s)",
        loaded["graph"],
        loaded["graph_fingerprints"]["edges"],
    )

    start = time.monotonic()
    for name in (*CONSTRAINTS, *INDEXES, "await_indexes"):
        s.run(f"constraints.{name}")
    have = {r["name"] for r in s.run("constraints.show_constraints")}
    check("constraints", set(CONSTRAINTS) <= have, str(sorted(have)))
    idx = {r["name"]: r for r in s.run("constraints.show_indexes")}
    online = all(
        idx.get(n, {}).get("state") == "ONLINE" and idx[n]["populationPercent"] == 100.0
        for n in (*CONSTRAINTS, *INDEXES)
    )
    check(
        "indexes_online",
        online,
        str({n: (r["state"], r["populationPercent"]) for n, r in idx.items()}),
    )
    log.info("constraints and indexes ready in %.1fs", time.monotonic() - start)

    rows = []
    for name, types in PROJECTIONS.items():
        want = expected[name]
        s.run("projections.drop", name=name)
        native = s.run(
            "projections.estimate_native", relationshipProjection=native_projection(types)
        )[0]
        check(
            f"{name}.estimate_fits_heap",
            native["bytesMax"] < heap,
            f"native upper bound {native['requiredMemory']}",
        )
        t0 = time.monotonic()
        built = s.run("projections.project_pairs", name=name, types=types)[0]
        seconds = time.monotonic() - t0
        info = s.run("projections.graph_info", name=name)[0]
        total = s.run("projections.weight_total", name=name)[0]
        top = [
            (r["a"], r["b"], r["w"])
            for r in s.run("projections.heaviest_pairs", name=name, k=TOP_PAIRS)
        ]
        check(
            f"{name}.nodes",
            built["nodes"] == want["nodes"],
            f"{built['nodes']} (Parquet {want['nodes']})",
        )
        check(
            f"{name}.relationships",
            built["relationships"] == 2 * want["pairs"],
            f"{built['relationships']} = 2 x {want['pairs']} pairs",
        )
        check(
            f"{name}.total_weight",
            total["total"] == 2 * want["total_weight"],
            f"{total['total']:.0f} = 2 x {want['total_weight']}",
        )
        check(
            f"{name}.heaviest_pairs",
            [(a, b, float(w)) for a, b, w in want["top"]] == top,
            str(top[:2]),
        )
        estimates = {a: s.run(f"estimates.{a}", graph=name, **analysis)[0] for a in ALGORITHMS}
        worst = max(estimates, key=lambda a: estimates[a]["bytesMax"])
        need = info["sizeInBytes"] + estimates[worst]["bytesMax"]
        check(
            f"{name}.graph_plus_algorithm_fits_heap",
            need < heap,
            f"{need / 2**30:.2f} GiB of {heap / 2**30:.0f} GiB (largest: {worst})",
        )
        rows.append(
            {
                "graph": loaded["graph"],
                "projection": name,
                "types": "+".join(types),
                "nodes": built["nodes"],
                "relationships": built["relationships"],
                "pairs": want["pairs"],
                "total_weight": want["total_weight"],
                "build_seconds": round(seconds, 1),
                "project_millis": built["project_millis"],
                "memory_usage": info["memoryUsage"],
                "size_bytes": info["sizeInBytes"],
                "native_estimate_max_bytes": native["bytesMax"],
                **{f"{a}_estimate_max_bytes": estimates[a]["bytesMax"] for a in ALGORITHMS},
                "heap_bytes": heap,
            }
        )
        log.info(
            "  %s: %d nodes, %d relationships in %.1fs, %s; estimates %s",
            name,
            built["nodes"],
            built["relationships"],
            seconds,
            info["memoryUsage"],
            {a: e["requiredMemory"] for a, e in estimates.items()},
        )
        if not keep:
            s.run("projections.drop", name=name)
    left = s.run("projections.list_graphs")[0]["graphs"]
    if not keep:
        check("projections_dropped", not left, str(left))
    driver.close()

    if loaded["graph"] == "full":
        repo_root = cfg.source.resolve().parent.parent
        target = repo_root / cfg.get("reports_dir") / "tables" / "gds_projections.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        log.info("report -> %s", target)
    if problems:
        raise RuntimeError(f"{len(problems)} GDS setup checks failed: " + "; ".join(problems))
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument(
        "--keep-projections", action="store_true", help="leave the projections in the GDS catalog"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # "IF NOT EXISTS ... already exists" notices are expected on re-runs.
    logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)
    start = time.monotonic()
    try:
        run(load_config(args.config), args.keep_projections)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
