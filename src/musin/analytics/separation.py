"""Degrees of separation between artists (F4.5, FR-19).

    docker compose exec spark python -m musin.analytics.separation --config config/pipeline.yaml \
        --pair "<artist A>" "<artist B>" [--pair ...]

An artist is given by MBID or by exact name. An ambiguous name is refused
with the candidates listed, so the caller can pass an MBID instead.

Reads:  the graph loaded in Neo4j ($MUSIN_DATA/neo4j/loaded_graph.json, F3.4)
Writes: <results>/separation_paths.csv  one row per --pair of this run (overwritten)
        <results> = reports/tables (full graph) or the sample/variant tree

For each pair it finds every shortest path (unweighted hops over the four
collaboration relationship types, never through :Genre) up to
`analysis.separation_max_paths`, and reports the hop count, the number of
distinct shortest artist sequences, and a canonical path: the sequence with
the smallest artistIds in order (D-036). Cypher's shortestPath returns an
arbitrary one of several tied paths; the canonical pick is the same on every
run. Exits 1 if an artist can't be resolved.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import pandas as pd

from musin.analytics.common import (
    CONNECT_WAIT_S,
    check_still_loaded,
    loaded_graph,
    results_dir,
    write_table,
)
from musin.common.config import Config, load_config
from musin.common.neo4j import get_driver
from musin.gds.setup import Session

log = logging.getLogger("musin.analytics.separation")

CYPHER_FILES = ("06_separation",)


def resolve(candidates: list[dict], key: str) -> dict:
    """The one artist matching `key`; RuntimeError if none or several.

    In: the rows of `separation.resolve` for `key`. An MBID match wins over
    name matches (MBIDs are unique). Several name matches are refused with
    every candidate listed, because picking one silently could change the
    answer.
    """
    by_mbid = [c for c in candidates if c["mbid"] == key]
    if by_mbid:
        return by_mbid[0]
    if not candidates:
        raise RuntimeError(f"no artist with MBID or exact name {key!r}")
    if len(candidates) > 1:
        listed = "; ".join(
            f"{c['mbid']} ({c['type'] or 'no type'}, {c['recordingCount']} recordings, "
            f"{c['primaryGenre'] or 'no genre'})"
            for c in candidates
        )
        raise RuntimeError(f"{len(candidates)} artists are named {key!r}; pass an MBID: {listed}")
    return candidates[0]


def canonical(sequences: list[list[int]]) -> tuple[list[int] | None, int]:
    """(smallest distinct artist sequence, number of distinct sequences).

    In: the artistId sequences of all shortest relationship paths. Parallel
    relationships repeat a sequence; they count once. (None, 0) if no path.
    """
    distinct = sorted({tuple(s) for s in sequences})
    return (list(distinct[0]) if distinct else None), len(distinct)


def describe_hop(a: str, b: str, relations: list[dict]) -> str:
    """'A -[FEATURED→ 3, CO_CREDITED 1]- B': every relationship between two path neighbours.

    In: relations with type, weight (shared recordings) and forward (True if
    the relationship points from a to b). Undirected CO_CREDITED gets no arrow.
    """
    parts = []
    for r in relations:
        arrow = "" if r["type"] == "CO_CREDITED" else ("→" if r["forward"] else "←")
        parts.append(f"{r['type']}{arrow} {r['weight']}")
    return f"{a} -[{', '.join(parts)}]- {b}"


def run(cfg: Config, pairs: list[tuple[str, str]]) -> pd.DataFrame:
    """Answer every pair on the loaded graph, write the table, and return it."""
    loaded = loaded_graph(cfg)
    max_paths = int(cfg.get("analysis.separation_max_paths"))
    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    log.info(
        "graph in Neo4j: %s (edges fingerprint %s); max paths %d", graph, fingerprint, max_paths
    )

    rows = []
    driver = get_driver(CONNECT_WAIT_S)
    try:
        s = Session(driver, files=CYPHER_FILES)
        for key_a, key_b in pairs:
            a = resolve(s.run("separation.resolve", key=key_a), key_a)
            b = resolve(s.run("separation.resolve", key=key_b), key_b)
            t0 = time.monotonic()
            found = s.run(
                "separation.shortest_paths",
                **{"from": a["artistId"], "to": b["artistId"]},
                maxPaths=max_paths,
            )[0]
            path, n_paths = canonical(found["sequences"])
            seconds = time.monotonic() - t0
            row = {
                "from_mbid": a["mbid"],
                "from_name": a["name"],
                "to_mbid": b["mbid"],
                "to_name": b["name"],
                "hops": None if path is None else len(path) - 1,
                "shortest_paths": n_paths,
                "capped": found["relationshipPaths"] >= max_paths,
                "path_names": None,
                "path_mbids": None,
                "hop_relations": None,
                "seconds": round(seconds, 2),
            }
            if path is not None:
                names = {r["artistId"]: r for r in s.run("separation.artists_by_id", ids=path)}
                relations = s.run("separation.hop_relations", ids=path)
                hops = [
                    describe_hop(
                        names[path[i]]["name"],
                        names[path[i + 1]]["name"],
                        [r for r in relations if r["hop"] == i],
                    )
                    for i in range(len(path) - 1)
                ]
                row["path_names"] = " > ".join(names[i]["name"] for i in path)
                row["path_mbids"] = " > ".join(names[i]["mbid"] for i in path)
                row["hop_relations"] = " | ".join(hops)
            rows.append(row)
            if path is None:
                log.info("%s -> %s: no path (different connected components)", a["name"], b["name"])
            else:
                log.info(
                    "%s -> %s: %d hops, %d shortest path%s%s (%.2fs)",
                    a["name"],
                    b["name"],
                    row["hops"],
                    n_paths,
                    "" if n_paths == 1 else "s",
                    " (cap reached)" if row["capped"] else "",
                    seconds,
                )
                for h in row["hop_relations"].split(" | "):
                    log.info("    %s", h)
    finally:
        driver.close()
    check_still_loaded(cfg, loaded)

    frame = pd.DataFrame(rows).assign(graph=graph, edges_fingerprint=fingerprint)
    target = write_table(frame, results_dir(cfg, graph) / "separation_paths.csv")
    log.info("separation_paths %d rows -> %s", len(frame), target)
    return frame


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument(
        "--pair",
        nargs=2,
        action="append",
        required=True,
        metavar=("ARTIST_A", "ARTIST_B"),
        help="two artists by MBID or exact name; repeat for more pairs",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    start = time.monotonic()
    try:
        run(load_config(args.config), [tuple(p) for p in args.pair])
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
