"""Validation against the pre-registered artist list (F4.6, FR-20, PRD §5.6).

    docker compose exec spark python -m musin.analytics.validation --config config/pipeline.yaml

Reads:  `validation.artists_file` (config/validation_artists.csv, committed before
        any RQ1 result, D-037) and the P4 scores on the loaded graph's :Artist nodes
        (F4.1 centrality, F4.2 louvain, F4.3 genreDiversity/bridgeScore)
Writes: <results>/validation_ranks.csv    where each listed artist ranks on every measure
        <results>/validation_summary.csv  how many listed artists reach each top-k
        <results>/validation_top10.csv    the top 10 of every ranking, for the manual
                                          artefact spot-check (PRD §5.6)
        <results> = reports/tables (full graph), else the sample/variant tree

Ranks are 1 = highest, ties broken by artistId as in the F4.1/F4.3 top lists,
over the artists for which the measure is defined. Exits 1 if a score
is missing from Neo4j (run F4.1-F4.3 first).
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

log = logging.getLogger("musin.analytics.validation")

CYPHER_FILES = ("08_validation",)
# measure -> the stage that writes it; bridgeScore first, it is what §5.6 validates
MEASURES = {
    "bridgeScore": "F4.3",
    "genreDiversity": "F4.3",
    "betweenness": "F4.1",
    "pagerank": "F4.1",
    "degreeWeighted": "F4.1",
}
TOP_KS = (10, 25, 100, 1000)
SPOT_CHECK = 10


def ranks(scores: pd.DataFrame, measure: str) -> pd.DataFrame:
    """Rank and percentile of `measure` among artists where it is defined.

    In: artistId and `measure` (null = undefined). Out: artistId,
    <measure>_rank (1 = highest; ties by artistId) and <measure>_pct (share of
    defined artists ranked at or below, so the top artist has 1.0).
    """
    defined = scores.loc[scores[measure].notna(), ["artistId", measure]]
    ordered = defined.sort_values([measure, "artistId"], ascending=[False, True], kind="mergesort")
    n = len(ordered)
    return pd.DataFrame(
        {
            "artistId": ordered["artistId"].to_numpy(),
            f"{measure}_rank": range(1, n + 1),
            f"{measure}_pct": [(n - i) / n for i in range(n)],
        }
    )


def summarise(table: pd.DataFrame, defined: dict[str, int]) -> pd.DataFrame:
    """Per measure: listed artists in the graph, with the measure defined, in each top-k.

    In: validation_ranks rows (one per listed artist) and the number of
    artists with each measure defined. Out: one row per measure with
    listed_defined, in_top_<k> for TOP_KS, median_rank and median_pct.
    """
    rows = []
    for m in MEASURES:
        r = table[f"{m}_rank"].dropna()
        rows.append(
            {
                "measure": m,
                "artists_with_measure": defined[m],
                "listed": len(table),
                "listed_in_graph": int(table["in_graph"].sum()),
                "listed_defined": len(r),
                **{f"in_top_{k}": int((r <= k).sum()) for k in TOP_KS},
                "median_rank": float(r.median()) if len(r) else None,
                "median_pct": float(table[f"{m}_pct"].dropna().median()) if len(r) else None,
            }
        )
    return pd.DataFrame(rows)


def run(cfg: Config) -> dict[str, pd.DataFrame]:
    """Run F4.6 on the loaded graph; return its tables by file stem."""
    loaded = loaded_graph(cfg)
    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    listed = pd.read_csv(cfg.source.resolve().parent.parent / cfg.get("validation.artists_file"))
    if listed["mbid"].duplicated().any():
        raise RuntimeError("validation list has duplicate MBIDs")
    log.info(
        "graph in Neo4j: %s (edges fingerprint %s); %d listed artists",
        graph,
        fingerprint,
        len(listed),
    )

    driver = get_driver(CONNECT_WAIT_S)
    try:
        scores = Session(driver, files=CYPHER_FILES).frame("validation.scores")
    finally:
        driver.close()
    check_still_loaded(cfg, loaded)
    missing = [f"{m} ({stage})" for m, stage in MEASURES.items() if scores[m].notna().sum() == 0]
    if missing:
        raise RuntimeError(
            f"no artist has {', '.join(missing)}: run those stages on this graph first"
        )

    table = (
        listed[["mbid", "name"]]
        .rename(columns={"name": "listed_name"})
        .merge(scores, on="mbid", how="left")
    )
    table["in_graph"] = table["artistId"].notna()
    for m in MEASURES:
        table = table.merge(ranks(scores, m), on="artistId", how="left")
    cols = ["mbid", "listed_name", "in_graph", "type", "primaryGenre", "recordingCount", "louvain"]
    for m in MEASURES:
        cols += [m, f"{m}_rank", f"{m}_pct"]
    table = table[cols].rename(
        columns={"primaryGenre": "primary_genre", "recordingCount": "recording_count"}
    )

    defined = {m: int(scores[m].notna().sum()) for m in MEASURES}
    top10 = []
    for m in MEASURES:
        r = ranks(scores, m).head(SPOT_CHECK).merge(scores, on="artistId")
        top10.append(
            r[
                [
                    "artistId",
                    "mbid",
                    "name",
                    "type",
                    "primaryGenre",
                    "recordingCount",
                    m,
                    f"{m}_rank",
                ]
            ]
            .rename(columns={m: "value", f"{m}_rank": "rank"})
            .assign(measure=m)
        )
    top = pd.concat(top10, ignore_index=True)[
        [
            "measure",
            "rank",
            "artistId",
            "mbid",
            "name",
            "type",
            "primaryGenre",
            "recordingCount",
            "value",
        ]
    ].rename(
        columns={
            "artistId": "artist_id",
            "primaryGenre": "primary_genre",
            "recordingCount": "recording_count",
        }
    )

    stamp = {"graph": graph, "edges_fingerprint": fingerprint}
    tables = {
        "validation_ranks": table,
        "validation_summary": summarise(table, defined),
        "validation_top10": top,
    }
    out = results_dir(cfg, graph)
    for stem, frame in tables.items():
        frame = frame.assign(**stamp)
        tables[stem] = frame
        log.info("%-18s %4d rows -> %s", stem, len(frame), write_table(frame, out / f"{stem}.csv"))
    for r in tables["validation_ranks"].itertuples():
        log.info(
            "  %-18s bridge %s  diversity %s  betweenness %s",
            r.listed_name,
            _rank(r.bridgeScore_rank, defined["bridgeScore"]),
            _rank(r.genreDiversity_rank, defined["genreDiversity"]),
            _rank(r.betweenness_rank, defined["betweenness"]),
        )
    return tables


def _rank(rank: float, n: int) -> str:
    return "      —      " if pd.isna(rank) else f"{int(rank):>6d}/{n:<6d}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
