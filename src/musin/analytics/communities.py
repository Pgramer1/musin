"""Louvain communities compared with genres (F4.2, FR-16, RQ2).

    docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml

Reads:  the graph loaded in Neo4j ($MUSIN_DATA/neo4j/loaded_graph.json, F3.4)
Writes: :Artist.louvain (community size rank of the run of record, PRD §8.3)
        <results>/communities_runs.csv       modularity, counts, NMI/AMI vs genre per run
        <results>/communities_stability.csv  NMI between every pair of runs
        <results>/communities_sizes.csv      community-size distribution (run of record)
        <results>/communities_top.csv        genre composition and example artists of
                                             the largest communities (run of record)
        <results> = reports/tables (full graph) or the sample tree (analytics.common)

Builds collab_undirected (F3.5), checks the heap estimate, and runs weighted
Louvain once single-threaded (the run of record, reproducible) and
`analysis.louvain_runs` more times at `analysis.concurrency` to measure how
much the partition depends on processing order (D-034). Communities are
compared with each labelled artist's primary genre by NMI (PRD §5.5) and AMI,
its chance-corrected variant, over all artists and inside the largest
connected component, where Louvain divides one connected network instead of
returning small separate components (D-034). The run of record is written back to Neo4j with
communities numbered by size. Exits 1 if a check fails.
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from itertools import combinations

import pandas as pd
from sklearn.metrics import adjusted_mutual_info_score, normalized_mutual_info_score

from musin.analytics.common import (
    CONNECT_WAIT_S,
    check_still_loaded,
    loaded_graph,
    results_dir,
    write_table,
)
from musin.common.config import Config, load_config, require_env
from musin.common.neo4j import get_driver
from musin.gds.setup import Session, drop, parse_size, project

log = logging.getLogger("musin.analytics.communities")

GRAPH = "collab_undirected"
RECORD = "record"  # name of the run of record; stability runs are stability_1 ...
CYPHER_FILES = ("02_projections", "04_centrality", "05_communities")
WRITE_BATCH = 50_000


def relabel_by_size(assign: pd.DataFrame) -> pd.DataFrame:
    """Number communities by size: 1 = largest, ties by the smallest artistId.

    In: artistId, community (GDS id, arbitrary). Out: artistId, community
    (rank). GDS ids depend on processing order; the rank does not, so tables,
    write-back and the report all use it.
    """
    stats = assign.groupby("community")["artistId"].agg(["size", "min"]).reset_index()
    stats = stats.sort_values(["size", "min"], ascending=[False, True], kind="mergesort")
    stats["rank"] = range(1, len(stats) + 1)
    mapping = dict(zip(stats["community"], stats["rank"], strict=True))
    return assign.assign(community=assign["community"].map(mapping))


def size_bucket(size: int) -> str:
    """Power-of-ten bucket label: 1 -> '1', 7 -> '2-9', 42 -> '10-99', 250 -> '100-999'."""
    if size < 2:
        return "1"
    low = 10 ** int(math.log10(size))
    return f"{max(low, 2)}-{low * 10 - 1}"


def size_distribution(assign: pd.DataFrame) -> pd.DataFrame:
    """Communities and artists per power-of-ten size bucket.

    In: artistId, community. Out: min_size, bucket, communities, artists,
    artist_share; ordered from the smallest bucket.
    """
    sizes = assign.groupby("community").size()
    frame = pd.DataFrame({"size": sizes.values})
    frame["bucket"] = frame["size"].map(size_bucket)
    frame["min_size"] = frame["bucket"].map(lambda b: int(b.split("-")[0]))
    out = (
        frame.groupby(["min_size", "bucket"])
        .agg(communities=("size", "size"), artists=("size", "sum"))
        .reset_index()
    )
    out["artist_share"] = out["artists"] / out["artists"].sum()
    return out.reset_index(drop=True)


def genre_agreement(assign: pd.DataFrame) -> dict:
    """NMI and AMI between community and primary genre, labelled artists only.

    In: community, primaryGenre (null = unlabelled). Out: labelled_artists,
    nmi_genre (PRD §5.5; sklearn default: arithmetic-mean normalisation),
    ami_genre (corrected for chance; many small communities inflate NMI).
    """
    labelled = assign.dropna(subset=["primaryGenre"])
    return {
        "labelled_artists": len(labelled),
        "nmi_genre": float(
            normalized_mutual_info_score(labelled["primaryGenre"], labelled["community"])
        ),
        "ami_genre": float(
            adjusted_mutual_info_score(labelled["primaryGenre"], labelled["community"])
        ),
    }


def largest_component(component: pd.Series) -> pd.Index:
    """artistIds of the largest connected component (ties: the one holding the smallest id).

    In: component id by artistId.
    """
    stats = (
        component.reset_index().groupby(component.name)[component.index.name].agg(["size", "min"])
    )
    best = stats.sort_values(["size", "min"], ascending=[False, True], kind="mergesort").index[0]
    return component.index[component == best]


def stability(partitions: dict[str, pd.Series], lcc: pd.Index) -> list[dict]:
    """NMI between every pair of partitions, over all artists and inside `lcc`.

    In: community by artistId per run; artistIds of the largest component.
    Outside it nearly every community is a whole small component and agrees
    trivially between runs, so `nmi_lcc` is the informative number.
    """
    rows = []
    for a, b in combinations(partitions, 2):
        x, y = partitions[a].align(partitions[b])
        rows.append(
            {
                "run_a": a,
                "run_b": b,
                "artists": len(x),
                "nmi": float(normalized_mutual_info_score(x, y)),
                "lcc_artists": len(lcc),
                "nmi_lcc": float(normalized_mutual_info_score(x[lcc], y[lcc])),
            }
        )
    return rows


def top_communities(assign: pd.DataFrame, genres: list[str], k: int, examples: int) -> pd.DataFrame:
    """Genre composition of the k largest communities (PRD §5 RQ2).

    In: artistId, community (size rank), name, primaryGenre, degreeWeighted.
    Out: one row per community 1..k with size, labelled, labelled_share,
    top_genre and top_genre_share (among labelled members), share_<genre> for
    every genre in `genres` (shares of labelled members, summing to 1), and
    example_artists: the `examples` members with the highest weighted degree
    (ties by artistId), joined by "; ".
    """
    top = assign[assign["community"] <= k]
    rows = []
    for community, members in top.groupby("community", sort=True):
        labelled = members.dropna(subset=["primaryGenre"])
        shares = labelled["primaryGenre"].value_counts(normalize=True)
        best = members.sort_values(
            ["degreeWeighted", "artistId"], ascending=[False, True], kind="mergesort"
        ).head(examples)
        row = {
            "community": community,
            "size": len(members),
            "labelled": len(labelled),
            "labelled_share": len(labelled) / len(members),
            "top_genre": shares.index[0] if len(shares) else None,
            "top_genre_share": float(shares.iloc[0]) if len(shares) else None,
        }
        row.update({f"share_{g}": float(shares.get(g, 0.0)) for g in genres})
        row["example_artists"] = "; ".join(best["name"].fillna("").astype(str))
        rows.append(row)
    return pd.DataFrame(rows)


def run_louvain(s: Session, prop: str, concurrency: int) -> tuple[pd.Series, dict]:
    """One mutate run under property `prop`; return (community by nodeId, stats)."""
    t0 = time.monotonic()
    stats = s.run("communities.louvain", graph=GRAPH, property=prop, concurrency=concurrency)[0]
    stats["wall_seconds"] = round(time.monotonic() - t0, 1)
    col = s.frame("centrality.stream_property", graph=GRAPH, property=prop)
    log.info("  %-12s concurrency %d  %5.1fs  %s", prop, concurrency, stats["wall_seconds"], stats)
    return col.set_index("nodeId")["value"].astype("int64"), stats


def run(cfg: Config) -> dict[str, pd.DataFrame]:
    """Run F4.2 on the loaded graph and write its tables; return them by file stem."""
    loaded = loaded_graph(cfg)
    heap = parse_size(require_env("NEO4J_HEAP_SIZE"))
    record_concurrency = int(cfg.get("analysis.louvain_concurrency"))
    concurrency = int(cfg.get("analysis.concurrency"))
    n_stability = int(cfg.get("analysis.louvain_runs"))
    k = int(cfg.get("analysis.top_k_communities"))
    examples = int(cfg.get("analysis.community_example_artists"))
    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    log.info(
        "graph in Neo4j: %s (edges fingerprint %s); record concurrency %d, %d stability runs "
        "at concurrency %d",
        graph,
        fingerprint,
        record_concurrency,
        n_stability,
        concurrency,
    )

    driver = get_driver(CONNECT_WAIT_S)
    try:
        s = Session(driver, files=CYPHER_FILES)
        artists = s.frame("centrality.artists")
        genres = [r["genre"] for r in s.run("communities.genres")]
        drop(driver, GRAPH)
        built = project(driver, GRAPH)
        try:
            info = s.run("projections.graph_info", name=GRAPH)[0]
            estimate = s.run("communities.estimate_louvain", graph=GRAPH, concurrency=concurrency)[
                0
            ]
            need = info["sizeInBytes"] + estimate["bytesMax"]
            log.info(
                "%s: %d nodes, %d relationships, %s; Louvain estimate %s",
                GRAPH,
                built["nodes"],
                built["relationships"],
                info["memoryUsage"],
                estimate["requiredMemory"],
            )
            if need >= heap:
                raise RuntimeError(f"{need / 2**30:.2f} GiB needed, heap {heap / 2**30:.0f} GiB")

            plan = [(RECORD, record_concurrency)] + [
                (f"stability_{i}", concurrency) for i in range(1, n_stability + 1)
            ]
            partitions, run_stats = {}, {}
            for name, c in plan:
                partitions[name], run_stats[name] = run_louvain(s, f"louvain_{name}", c)
                run_stats[name]["concurrency"] = c
            s.run("centrality.degree", graph=GRAPH, concurrency=concurrency)
            degree = s.frame("centrality.stream_property", graph=GRAPH, property="degreeWeighted")
            wcc = s.run("communities.wcc", graph=GRAPH, concurrency=concurrency)[0]
            component = s.frame("centrality.stream_property", graph=GRAPH, property="component")
        finally:
            drop(driver, GRAPH)

        ids = artists.set_index("nodeId")["artistId"]
        partitions = {
            name: p.rename(index=ids).rename_axis("artistId") for name, p in partitions.items()
        }
        problems = [
            f"{name}: {len(p)} artists assigned, {built['nodes']} projected"
            for name, p in partitions.items()
            if len(p) != built["nodes"] or p.isna().any()
        ]
        if problems:
            raise RuntimeError("; ".join(problems))
        component = (
            component.set_index("nodeId")["value"]
            .rename(index=ids)
            .rename_axis("artistId")
            .rename("component")
        )
        lcc = largest_component(component)
        log.info(
            "%d connected components; largest holds %d artists (%.1f %%)",
            wcc["componentCount"],
            len(lcc),
            100 * len(lcc) / len(component),
        )

        record = relabel_by_size(partitions[RECORD].rename("community").reset_index()).merge(
            artists.drop(columns="nodeId"), on="artistId", how="left"
        )
        record = record.merge(
            degree.merge(artists[["nodeId", "artistId"]], on="nodeId")[
                ["artistId", "value"]
            ].rename(columns={"value": "degreeWeighted"}),
            on="artistId",
            how="left",
        )

        t0 = time.monotonic()
        rows = record[["artistId", "community"]].astype("int64").to_dict("records")
        for i in range(0, len(rows), WRITE_BATCH):
            s.run("communities.write_louvain", rows=rows[i : i + WRITE_BATCH])
        counts = s.run("communities.count_written")[0]
        if counts["louvain"] != counts["artists"]:
            raise RuntimeError(f"write-back incomplete: {counts}")
        log.info("wrote louvain for %d artists in %.1fs", counts["louvain"], time.monotonic() - t0)
    finally:
        driver.close()
    check_still_loaded(cfg, loaded)

    genre_of = artists.set_index("artistId")["primaryGenre"]
    run_rows = []
    for name, p in partitions.items():
        frame = pd.DataFrame({"community": p, "primaryGenre": genre_of.reindex(p.index)})
        sizes = frame.groupby("community").size().sort_values(ascending=False)
        inside = frame.loc[lcc]
        st = run_stats[name]
        run_rows.append(
            {
                "run": name,
                "concurrency": st["concurrency"],
                "modularity": st["modularity"],
                "communities": st["communityCount"],
                "ran_levels": st["ranLevels"],
                "largest_community": int(sizes.iloc[0]),
                "top_k_artist_share": float(sizes.iloc[:k].sum() / len(frame)),
                **genre_agreement(frame),
                "components": wcc["componentCount"],
                "lcc_artists": len(lcc),
                "lcc_communities": inside["community"].nunique(),
                **{f"lcc_{key}": v for key, v in genre_agreement(inside).items()},
                "compute_millis": st["computeMillis"],
                "max_levels": st["maxLevels"],
                "max_iterations": st["maxIterations"],
                "tolerance": st["tolerance"],
            }
        )

    stamp = {"graph": graph, "edges_fingerprint": fingerprint}
    tables = {
        "communities_runs": pd.DataFrame(run_rows),
        "communities_stability": pd.DataFrame(stability(partitions, lcc)),
        "communities_sizes": size_distribution(record),
        "communities_top": top_communities(record, genres, k, examples),
    }
    out = results_dir(cfg, graph)
    for stem, frame in tables.items():
        frame = frame.assign(**stamp)
        tables[stem] = frame
        log.info("%-22s %4d rows -> %s", stem, len(frame), write_table(frame, out / f"{stem}.csv"))
    for r in run_rows:
        log.info(
            "  %-12s modularity %.4f  communities %d  NMI %.4f  AMI %.4f | largest component: "
            "%d communities, NMI %.4f  AMI %.4f",
            r["run"],
            r["modularity"],
            r["communities"],
            r["nmi_genre"],
            r["ami_genre"],
            r["lcc_communities"],
            r["lcc_nmi_genre"],
            r["lcc_ami_genre"],
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
