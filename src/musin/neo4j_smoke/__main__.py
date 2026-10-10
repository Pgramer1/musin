"""Neo4j + GDS smoke test on the toy graph (F1.5, FR-14 prep).

    bash neo4j/smoke/run_smoke.sh --overwrite      # import + this check (WSL)
    docker compose exec spark python -m musin.neo4j_smoke --config config/pipeline.yaml

Run after neo4j/smoke/run_smoke.sh has bulk-imported the toy CSVs into the
`neo4j` database. Reads the same CSVs from $MUSIN_DATA/<paths.import>/toy/,
computes reference answers in plain Python (reference.py), and checks:
import types and counts, constraints, the COLLAB projection, weighted degree,
PageRank, betweenness, Louvain, memory estimation, write-back, shortest path,
and that projections are dropped again. Exits non-zero if any check fails.
Writes nothing outside Neo4j apart from the `pagerank` property on the toy
artists.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter, deque
from pathlib import Path

from neo4j import Driver

from musin.common.config import Config, load_config
from musin.common.cypher import load_queries
from musin.common.neo4j import get_driver
from musin.neo4j_smoke import reference as ref

log = logging.getLogger("musin.neo4j_smoke")

CYPHER_DIR = Path(__file__).resolve().parents[3] / "neo4j" / "smoke"
TOY_SUBDIR = "toy"
EDGE_FILES = ("co_credited", "featured", "produced_for", "performed_for")
# ID groups whose node file declares {id-type:long} (artistId is an integer, PRD §8.3).
INT_ID_GROUPS = frozenset({"Artist"})
CONNECT_WAIT_S = 120
# Float comparison against the reference implementation.
ABS_TOL = 1e-6
# PageRank run to convergence for the reference comparison; the PRD §5.5
# setting (max_iterations from config) is run and reported separately.
CONVERGED_TOLERANCE = 1e-10
CONVERGED_MAX_ITERATIONS = 1000
LOUVAIN_CONCURRENCIES = (1, 4)
PATH_FROM, PATH_TO = 101, 112  # opposite ends of the toy graph


class Checks:
    """Collects pass/fail results so one run reports every failure."""

    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((name, ok, detail))
        (log.info if ok else log.error)("%s %s %s", "PASS" if ok else "FAIL", name, detail)
        return ok

    @property
    def failed(self) -> list[str]:
        return [name for name, ok, _ in self.results if not ok]


def run_query(driver: Driver, query: str, **params) -> list[dict]:
    records, _, _ = driver.execute_query(query, params, database_="neo4j")
    return [record.data() for record in records]


def max_abs_diff(a: dict[int, float], b: dict[int, float]) -> float:
    if a.keys() != b.keys():
        return float("inf")
    return max(abs(a[k] - b[k]) for k in a)


def hops(pairs: dict[ref.Pair, float], src: int, dst: int) -> int:
    """Unweighted BFS distance in the merged graph."""
    adj = ref.neighbours(pairs)
    dist, queue = {src: 0}, deque([src])
    while queue:
        v = queue.popleft()
        for w in adj[v]:
            if w not in dist:
                dist[w] = dist[v] + 1
                queue.append(w)
    return dist[dst]


def by_artist(rows: list[dict], column: str) -> dict[int, float]:
    return {row["artistId"]: row[column] for row in rows}


def run(cfg: Config) -> Checks:
    toy = cfg.data_path("import") / TOY_SUBDIR
    artists = ref.read_import_csv(toy / "artists.csv", INT_ID_GROUPS)
    genres = ref.read_import_csv(toy / "genres.csv", INT_ID_GROUPS)
    in_genre = ref.read_import_csv(toy / "in_genre.csv", INT_ID_GROUPS)
    edges = [
        row
        for kind in EDGE_FILES
        for row in ref.read_import_csv(toy / f"edges_{kind}.csv", INT_ID_GROUPS)
    ]
    triples = [(e["START_ID"], e["END_ID"], e["weight"]) for e in edges]
    pairs = ref.merged_pairs(triples)
    multiplicity = Counter((min(s, d), max(s, d)) for s, d, _ in triples)
    log.info(
        "toy CSVs in %s: %d artists, %d genres, %d IN_GENRE, %d edges -> %d merged pairs",
        toy,
        len(artists),
        len(genres),
        len(in_genre),
        len(edges),
        len(pairs),
    )

    q = {}
    for path in sorted(CYPHER_DIR.glob("*.cypher")):
        q.update(load_queries(path))
    c = Checks()
    driver = get_driver(CONNECT_WAIT_S)
    try:
        # --- server and import -------------------------------------------------
        for name in ("artist_id_unique", "genre_name_unique", "artist_mbid_index", "await_indexes"):
            run_query(driver, q[name])
        v = run_query(driver, q["versions"])[0]
        log.info("server: %s %s %s, GDS %s", v["name"], v["version"], v["edition"], v["gds"])

        labels = {r["label"]: r["n"] for r in run_query(driver, q["label_counts"])}
        c.check(
            "node counts", labels == {"Artist": len(artists), "Genre": len(genres)}, str(labels)
        )
        want_types = Counter(e["TYPE"] for e in edges) + Counter(r["TYPE"] for r in in_genre)
        types = {r["type"]: r["n"] for r in run_query(driver, q["rel_type_counts"])}
        c.check("relationship counts", types == dict(want_types), str(types))

        props = {r["artistId"]: r for r in run_query(driver, q["artist_property_types"])}
        id_types = {r["artistIdType"] for r in props.values()}
        c.check("artistId is an integer property", id_types == {"INTEGER NOT NULL"}, str(id_types))
        c.check(
            "recordingCount is an integer property",
            {r["recordingCountType"] for r in props.values()} == {"INTEGER NOT NULL"},
        )
        empty_area = {a["artistId"] for a in artists if a["areaName"] is None}
        missing_area = {k for k, r in props.items() if r["areaNameMissing"]}
        c.check(
            "empty CSV fields become missing properties",
            missing_area == empty_area,
            f"missing areaName: {sorted(missing_area)}",
        )
        names_ok = all(props[a["artistId"]]["name"] == a["name"] for a in artists)
        c.check(
            "quoted names survive the import",
            names_ok,
            str([a["name"] for a in artists if '"' in a["name"] or "," in a["name"]]),
        )

        db_edges = run_query(driver, q["edge_property_types"])
        want_edges = sorted(
            (e["START_ID"], e["END_ID"], e["TYPE"], e["weight"], e["firstYear"], e["lastYear"])
            for e in edges
        )
        got_edges = sorted(
            (r["src"], r["dst"], r["type"], r["weight"], r["firstYear"], r["lastYear"])
            for r in db_edges
        )
        c.check("edge endpoints, weights and years match the CSVs", got_edges == want_edges)
        c.check(
            "edge weight is an integer property",
            {r["weightType"] for r in db_edges} == {"INTEGER NOT NULL"},
        )
        constraints = {r["name"] for r in run_query(driver, q["constraints"])}
        c.check(
            "uniqueness constraints exist",
            {"smoke_artist_id", "smoke_genre_name"} <= constraints,
            str(sorted(constraints)),
        )

        # --- projections ------------------------------------------------------
        run_query(driver, q["drop_if_exists"])
        g = run_query(driver, q["collab_undirected"])[0]
        c.check(
            "collab_undirected projection size",
            g["nodes"] == len(artists) and g["relationships"] == 2 * len(pairs),
            f"{g['nodes']} nodes, {g['relationships']} relationships "
            f"(= 2 x {len(pairs)} merged pairs, GDS counts both directions)",
        )
        g = run_query(driver, q["collab_native"])[0]
        c.check(
            "collab_native projection size",
            g["nodes"] == len(artists) and g["relationships"] == 2 * len(edges),
            f"{g['nodes']} nodes, {g['relationships']} relationships (parallel kept)",
        )

        damping = float(cfg.get("analysis.pagerank.damping"))
        max_iter = int(cfg.get("analysis.pagerank.max_iterations"))
        sampling = int(cfg.get("analysis.betweenness_sampling_size"))
        seed = int(cfg.get("analysis.random_seed"))
        louvain_runs = int(cfg.get("analysis.louvain_runs"))
        graph = "collab_undirected"

        # --- weighted degree ---------------------------------------------------
        want_deg = ref.weighted_degree(pairs)
        for name in ("collab_undirected", "collab_native"):
            got = by_artist(run_query(driver, q["degree_weighted"], graph=name), "score")
            d = max_abs_diff(got, want_deg)
            c.check(f"weighted degree = reference ({name})", d < ABS_TOL, f"max |diff| {d:.2e}")

        # --- PageRank -----------------------------------------------------------
        est = run_query(
            driver, q["pagerank_estimate"], graph=graph, damping=damping, maxIterations=max_iter
        )[0]
        log.info(
            "pageRank estimate: %s (%d-%d bytes)",
            est["requiredMemory"],
            est["bytesMin"],
            est["bytesMax"],
        )
        c.check("pageRank .estimate runs", est["bytesMax"] > 0, est["requiredMemory"])
        want_pr = ref.pagerank(pairs, damping, CONVERGED_TOLERANCE)
        conv = {
            "graph": graph,
            "damping": damping,
            "maxIterations": CONVERGED_MAX_ITERATIONS,
            "tolerance": CONVERGED_TOLERANCE,
        }
        got_pr = by_artist(run_query(driver, q["pagerank"], **conv), "score")
        d = max_abs_diff(got_pr, want_pr)
        c.check("PageRank (converged) = reference", d < ABS_TOL, f"max |diff| {d:.2e}")
        prd = {"graph": graph, "damping": damping, "maxIterations": max_iter, "tolerance": 1e-7}
        stats = run_query(driver, q["pagerank_stats"], **prd)[0]
        prd_pr = by_artist(run_query(driver, q["pagerank"], **prd), "score")
        same_rank = sorted(prd_pr, key=prd_pr.get) == sorted(want_pr, key=want_pr.get)
        c.check(
            f"PageRank at max_iterations={max_iter} keeps the converged ranking",
            same_rank,
            f"ranIterations {stats['ranIterations']}, didConverge {stats['didConverge']}, "
            f"max |diff| vs converged {max_abs_diff(prd_pr, want_pr):.2e}",
        )
        top = sorted(want_pr, key=want_pr.get, reverse=True)[:3]
        log.info("PageRank top 3: %s", [(a, round(want_pr[a], 4)) for a in top])

        # --- betweenness --------------------------------------------------------
        est = run_query(
            driver, q["betweenness_estimate"], graph=graph, samplingSize=sampling, seed=seed
        )[0]
        c.check("betweenness .estimate runs", est["bytesMax"] > 0, est["requiredMemory"])
        want_bt = ref.betweenness({p: 1 for p in pairs})
        got_bt = by_artist(
            run_query(driver, q["betweenness"], graph=graph, samplingSize=sampling, seed=seed),
            "score",
        )
        d = max_abs_diff(got_bt, want_bt)
        c.check(
            "betweenness (unweighted, exact) = reference",
            d < ABS_TOL,
            f"max |diff| {d:.2e}; sampling size {sampling} >= {len(artists)} nodes",
        )
        top_bt = max(want_bt, key=want_bt.get)
        c.check(
            "bridge artist 106 has the highest betweenness",
            top_bt == 106,
            f"top {top_bt}: {want_bt[top_bt]:.3f}",
        )
        native_bt = by_artist(
            run_query(
                driver, q["betweenness"], graph="collab_native", samplingSize=sampling, seed=seed
            ),
            "score",
        )
        want_multi = ref.betweenness(dict(multiplicity))
        c.check(
            "native projection betweenness = multigraph reference",
            max_abs_diff(native_bt, want_multi) < ABS_TOL,
            f"max |diff| vs merged reference {max_abs_diff(native_bt, want_bt):.3f} "
            "(parallel edges change shortest-path counts)",
        )

        # --- Louvain ------------------------------------------------------------
        # Expected: no community mixes the two genre clusters (bridge 106 may go
        # either way), and the result is at least as modular as the best plain
        # genre split. Louvain may split a cluster further if that raises Q.
        genre_of = {a["artistId"]: a["primaryGenre"] for a in artists if a["artistId"] != 106}
        genre_ids = {g: i for i, g in enumerate(sorted(set(genre_of.values())))}
        split_q = max(
            ref.modularity(pairs, {**{a: genre_ids[g] for a, g in genre_of.items()}, 106: i})
            for i in genre_ids.values()
        )
        for conc in LOUVAIN_CONCURRENCIES:
            partitions = []
            for _ in range(louvain_runs):
                rows = run_query(driver, q["louvain"], graph=graph, concurrency=conc)
                partitions.append({r["artistId"]: r["communityId"] for r in rows})
            groups = [
                frozenset(frozenset(a for a in p if p[a] == cid) for cid in set(p.values()))
                for p in partitions
            ]
            c.check(
                f"Louvain stable over {louvain_runs} runs (concurrency {conc})",
                len(set(groups)) == 1,
                f"{len(set(groups))} distinct partitions",
            )
            p = partitions[0]
            communities = sorted(sorted(a for a in p if p[a] == cid) for cid in set(p.values()))
            mixed = [m for m in communities if len({genre_of[a] for a in m if a != 106}) > 1]
            c.check(
                f"Louvain communities stay within one genre cluster (concurrency {conc})",
                not mixed,
                f"{len(communities)} communities {communities}",
            )
            stats = run_query(driver, q["louvain_stats"], graph=graph, concurrency=conc)[0]
            want_q = ref.modularity(pairs, p)
            c.check(
                f"Louvain modularity = reference (concurrency {conc})",
                abs(stats["modularity"] - want_q) < ABS_TOL,
                f"GDS {stats['modularity']:.6f}, reference {want_q:.6f}, "
                f"levels {stats['ranLevels']}",
            )
            c.check(
                f"Louvain modularity >= best genre split (concurrency {conc})",
                stats["modularity"] >= split_q - ABS_TOL,
                f"{stats['modularity']:.6f} vs {split_q:.6f}",
            )

        # --- write-back, shortest path, cleanup --------------------------------
        written = run_query(driver, q["pagerank_write"], **conv)[0]["nodePropertiesWritten"]
        back = by_artist(run_query(driver, q["pagerank_read_back"]), "pagerank")
        c.check(
            "pagerank written back as Artist property",
            written == len(artists) and max_abs_diff(back, got_pr) < ABS_TOL,
            f"{written} properties written",
        )

        sp = run_query(driver, q["shortest_path"], **{"from": PATH_FROM, "to": PATH_TO})[0]
        want_hops = hops(pairs, PATH_FROM, PATH_TO)
        c.check(
            f"shortestPath {PATH_FROM} -> {PATH_TO}",
            sp["hops"] == want_hops,
            f"{sp['hops']} hops via {sp['path']}",
        )

        run_query(driver, q["drop_all"])
        left = run_query(driver, q["list_graphs"])[0]["graphs"]
        c.check("projections dropped", left == 0, f"{left} left")
    finally:
        driver.close()
    return c


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("neo4j").setLevel(logging.WARNING)
    start = time.monotonic()
    checks = run(load_config(args.config))
    passed = len(checks.results) - len(checks.failed)
    log.info("%d/%d checks passed in %.1fs", passed, len(checks.results), time.monotonic() - start)
    if checks.failed:
        log.error("failed: %s", ", ".join(checks.failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
