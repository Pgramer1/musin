"""Genre bridges: genre diversity, bridge score, genre x genre matrix (F4.3, FR-17, RQ1).

    docker compose exec spark python -m musin.analytics.bridges --config config/pipeline.yaml

Reads:  the graph loaded in Neo4j ($MUSIN_DATA/neo4j/loaded_graph.json) and, for
        genre diversity and the matrix, the same graph's Parquet (marker `source`)
        :Artist.betweenness, written by F4.1 (run `musin.analytics.centrality` first)
Writes: :Artist.genreDiversity, :Artist.bridgeScore (artists with a defined diversity, §8.3)
        <results>/bridges_top.csv       top `analysis.top_k_bridges` artists by bridge score
        <results>/bridges_summary.csv   counts and the diversity/bridge distributions
        <results>/genre_matrix.csv      labelled collaboration weight per genre pair
        <figures>/genre_matrix.png      the matrix, row-normalised, as a heatmap
        <figures>/bridge_ego_<n>.png    ego networks of the top `analysis.ego_figures` bridges
        <results>/<figures> = reports/{tables,figures} (full graph), else the sample/variant tree

PRD §5 RQ1, on the COLLAB pairs (D-017: all edge types, weight = shared recordings):
    D_i = 1 - sum_g p_ig^2, p_ig = share of i's labelled collaboration weight
          going to neighbours of primary genre g; defined with at least
          `analysis.min_labelled_neighbours` labelled neighbours
    B_i = D_i x percentile_rank(betweenness_i), percentile over all artists (D-038)
    genre matrix: every labelled pair adds its weight to (genre a, genre b), symmetric
Exits 1 if a check fails.
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

from musin.analytics.common import (
    CONNECT_WAIT_S,
    check_still_loaded,
    loaded_graph,
    results_dir,
    write_table,
)
from musin.common.config import Config, load_config
from musin.common.layers import figures_dir
from musin.common.neo4j import get_driver
from musin.common.spark import get_spark
from musin.gds.setup import Session

log = logging.getLogger("musin.analytics.bridges")

CYPHER_FILES = ("07_bridges",)
WRITE_BATCH = 50_000
PROFILE_GENRES = 3  # genres listed per bridge in the top table


def collab_pairs(edges: DataFrame) -> DataFrame:
    """COLLAB pairs (D-017): one row per unordered artist pair, weights of all types summed.

    In: edges (src_id, dst_id, weight). Out: a < b, w.
    """
    return edges.groupBy(
        F.least("src_id", "dst_id").alias("a"), F.greatest("src_id", "dst_id").alias("b")
    ).agg(F.sum("weight").alias("w"))


def neighbour_genres(pairs: DataFrame, nodes: DataFrame) -> DataFrame:
    """Labelled collaboration weight of each artist per neighbour genre.

    In: collab_pairs(), nodes_artist (artist_id, primary_genre). Out:
    artist_id, genre, w (summed weight to neighbours of that primary genre),
    neighbours (how many). Unlabelled neighbours are left out (PRD §5).
    """
    both = pairs.select(F.col("a").alias("artist_id"), F.col("b").alias("nb"), "w").unionByName(
        pairs.select(F.col("b").alias("artist_id"), F.col("a").alias("nb"), "w")
    )
    genre = nodes.select(F.col("artist_id").alias("nb"), F.col("primary_genre").alias("genre"))
    return (
        both.join(genre, "nb")
        .filter(F.col("genre").isNotNull())
        .groupBy("artist_id", "genre")
        .agg(F.sum("w").alias("w"), F.count("*").alias("neighbours"))
    )


def diversity(by_genre: DataFrame, min_labelled: int) -> DataFrame:
    """Genre diversity per artist (PRD §5 RQ1).

    In: neighbour_genres(). Out: artist_id, labelled_neighbours,
    labelled_weight, neighbour_genres (distinct genres), genre_diversity
    (1 - sum of squared weight shares; null below `min_labelled` labelled
    neighbours).
    """
    totals = by_genre.groupBy("artist_id").agg(
        F.sum("neighbours").alias("labelled_neighbours"),
        F.sum("w").alias("labelled_weight"),
        F.count("*").alias("neighbour_genres"),
    )
    shares = by_genre.join(totals, "artist_id").withColumn(
        "p2", F.pow(F.col("w") / F.col("labelled_weight"), 2)
    )
    out = shares.groupBy(
        "artist_id", "labelled_neighbours", "labelled_weight", "neighbour_genres"
    ).agg((F.lit(1.0) - F.sum("p2")).alias("d"))
    return out.withColumn(
        "genre_diversity",
        F.when(F.col("labelled_neighbours") >= min_labelled, F.col("d")),
    ).drop("d")


def genre_matrix(pairs: DataFrame, nodes: DataFrame) -> DataFrame:
    """Labelled collaboration weight per unordered genre pair (PRD §5 RQ1).

    In: collab_pairs(), nodes_artist. Out: genre_a <= genre_b, weight, pairs.
    A pair counts when both artists have a primary genre; A-B and B-A meet in
    one cell; genre_a = genre_b is within-genre collaboration.
    """
    g = nodes.select("artist_id", "primary_genre")
    labelled = (
        pairs.join(
            g.withColumnRenamed("artist_id", "a").withColumnRenamed("primary_genre", "ga"), "a"
        )
        .join(g.withColumnRenamed("artist_id", "b").withColumnRenamed("primary_genre", "gb"), "b")
        .filter(F.col("ga").isNotNull() & F.col("gb").isNotNull())
    )
    return labelled.groupBy(
        F.least("ga", "gb").alias("genre_a"), F.greatest("ga", "gb").alias("genre_b")
    ).agg(F.sum("w").alias("weight"), F.count("*").alias("pairs"))


def percentile_rank(values: pd.Series) -> pd.Series:
    """(artists strictly below + half the artists equal) / all artists (D-038).

    The mid-rank convention: the many artists with betweenness 0 share the
    same percentile, and the result does not depend on how ties are ordered.
    """
    n = len(values)
    below = values.rank(method="min") - 1
    equal = values.map(values.value_counts())
    return (below + 0.5 * equal) / n


def square_matrix(cells: pd.DataFrame, genres: list[str]) -> pd.DataFrame:
    """The symmetric genre x genre weight matrix from genre_matrix() cells.

    Out: long table genre_a, genre_b, weight, share (of all labelled weight)
    over every ordered pair of `genres`, zeros included; cell (a, b) equals
    cell (b, a). Each pair's weight counts once in the share's denominator.
    """
    weight = {(r.genre_a, r.genre_b): r.weight for r in cells.itertuples()}
    total = float(cells["weight"].sum())
    rows = []
    for a in genres:
        for b in genres:
            w = weight.get((min(a, b), max(a, b)), 0)
            rows.append(
                {"genre_a": a, "genre_b": b, "weight": int(w), "share": w / total if total else 0.0}
            )
    return pd.DataFrame(rows)


def genre_profile(rows: pd.DataFrame, k: int) -> str:
    """'rock 45%, jazz 30%, pop 10%': the k largest neighbour genres by weight share."""
    rows = rows.sort_values(["w", "genre"], ascending=[False, True], kind="mergesort")
    total = rows["w"].sum()
    return ", ".join(f"{r.genre} {r.w / total:.0%}" for r in rows.head(k).itertuples())


def run(cfg: Config) -> dict[str, pd.DataFrame]:
    """Run F4.3 on the loaded graph; return the tables by file stem."""
    from musin.analytics.bridge_figures import plot_ego, plot_matrix

    loaded = loaded_graph(cfg)
    graph, fingerprint = loaded["graph"], loaded["graph_fingerprints"]["edges"]
    min_labelled = int(cfg.get("analysis.min_labelled_neighbours"))
    k = int(cfg.get("analysis.top_k_bridges"))
    n_ego = int(cfg.get("analysis.ego_figures"))
    max_nb = int(cfg.get("analysis.ego_max_neighbours"))
    source = Path(loaded["source"])
    log.info("graph in Neo4j: %s (edges fingerprint %s); Parquet %s", graph, fingerprint, source)

    spark = get_spark("musin-analytics-bridges")
    nodes = spark.read.parquet(str(source / "nodes_artist"))
    pairs = collab_pairs(spark.read.parquet(str(source / "edges"))).cache()
    by_genre = neighbour_genres(pairs, nodes).cache()
    div_all = diversity(by_genre, min_labelled).cache()
    with_labelled = div_all.count()
    div = div_all.filter(F.col("genre_diversity").isNotNull()).toPandas()  # defined D only
    cells = genre_matrix(pairs, nodes).toPandas()  # ≤ 120 genre pairs
    genres = sorted(
        r["primary_genre"]
        for r in nodes.select("primary_genre").distinct().collect()
        if r["primary_genre"] is not None
    )
    n_nodes = nodes.count()

    driver = get_driver(CONNECT_WAIT_S)
    try:
        s = Session(driver, files=CYPHER_FILES)
        artists = s.frame("bridges.betweenness")
        if len(artists) != n_nodes:
            raise RuntimeError(f"Neo4j holds {len(artists)} artists, Parquet {n_nodes}")
        if artists["betweenness"].isna().any():
            raise RuntimeError(
                f"{int(artists['betweenness'].isna().sum())} artists have no betweenness: "
                "run musin.analytics.centrality (F4.1) on this graph first"
            )
        artists["betweenness_pct"] = percentile_rank(artists["betweenness"])
        scores = artists.merge(
            div.rename(columns={"artist_id": "artistId"}), on="artistId", how="inner"
        )
        scores = scores.copy()
        scores["bridge_score"] = scores["genre_diversity"] * scores["betweenness_pct"]
        scores = scores.sort_values(
            ["bridge_score", "artistId"], ascending=[False, True], kind="mergesort"
        )
        scores["bridge_rank"] = range(1, len(scores) + 1)

        t0 = time.monotonic()
        rows = [
            {
                "artistId": int(r.artistId),
                "genreDiversity": float(r.genre_diversity),
                "bridgeScore": float(r.bridge_score),
            }
            for r in scores.itertuples()
        ]
        for i in range(0, len(rows), WRITE_BATCH):
            s.run("bridges.write_bridges", rows=rows[i : i + WRITE_BATCH])
        counts = s.run("bridges.count_written")[0]
        if counts["genreDiversity"] != len(rows) or counts["bridgeScore"] != len(rows):
            raise RuntimeError(f"write-back: {counts}, expected {len(rows)} each")
        log.info(
            "wrote genreDiversity/bridgeScore for %d artists in %.1fs",
            len(rows),
            time.monotonic() - t0,
        )
    finally:
        driver.close()
    check_still_loaded(cfg, loaded)

    top = scores.head(k).copy()
    top_ids = top["artistId"].tolist()
    ego_ids = top_ids[:n_ego]
    profiles = by_genre.filter(F.col("artist_id").isin(top_ids)).toPandas()
    top["genre_profile"] = [
        genre_profile(profiles[profiles["artist_id"] == i], PROFILE_GENRES) for i in top_ids
    ]
    ego_edges = (
        pairs.filter(
            F.col("a").isin(ego_ids) | F.col("b").isin(ego_ids)
        ).toPandas()  # the ego artists' pairs only
    )
    spark.stop()

    out = results_dir(cfg, graph)
    figures = figures_dir(cfg, graph)
    info = artists.set_index("artistId")[["name", "primaryGenre"]]
    for n, ego in enumerate(ego_ids, start=1):
        edges = ego_edges[(ego_edges["a"] == ego) | (ego_edges["b"] == ego)]
        nb = pd.DataFrame(
            {"artistId": edges["b"].where(edges["a"] == ego, edges["a"]), "w": edges["w"]}
        ).join(info, on="artistId")
        target = plot_ego(
            info.loc[ego, "name"],
            info.loc[ego, "primaryGenre"],
            nb,
            max_nb,
            figures / f"bridge_ego_{n}.png",
            graph,
        )
        log.info(
            "ego network %d (%s, %d neighbours) -> %s", n, info.loc[ego, "name"], len(nb), target
        )
    matrix = square_matrix(cells, genres)
    log.info("genre matrix -> %s", plot_matrix(matrix, genres, figures / "genre_matrix.png", graph))

    q = scores["genre_diversity"].quantile([0.25, 0.5, 0.75, 0.9]).tolist()
    summary = pd.DataFrame(
        [
            {
                "artists": len(artists),
                "with_labelled_neighbour": with_labelled,
                "with_diversity": len(scores),
                "min_labelled_neighbours": min_labelled,
                "diversity_p25": q[0],
                "diversity_p50": q[1],
                "diversity_p75": q[2],
                "diversity_p90": q[3],
                "diversity_zero": int((scores["genre_diversity"] == 0).sum()),
                "betweenness_zero_share": float((artists["betweenness"] == 0).mean()),
                "labelled_pairs": int(cells["pairs"].sum()),
                "labelled_weight": int(cells["weight"].sum()),
                "within_genre_weight_share": float(
                    cells.loc[cells["genre_a"] == cells["genre_b"], "weight"].sum()
                    / cells["weight"].sum()
                ),
            }
        ]
    )
    top_table = top[
        [
            "bridge_rank",
            "artistId",
            "mbid",
            "name",
            "type",
            "primaryGenre",
            "genre_diversity",
            "labelled_neighbours",
            "neighbour_genres",
            "genre_profile",
            "betweenness",
            "betweenness_pct",
            "bridge_score",
        ]
    ].rename(columns={"artistId": "artist_id", "primaryGenre": "primary_genre"})
    stamp = {"graph": graph, "edges_fingerprint": fingerprint}
    tables = {"bridges_top": top_table, "bridges_summary": summary, "genre_matrix": matrix}
    for stem, frame in tables.items():
        frame = frame.assign(**stamp)
        tables[stem] = frame
        log.info("%-16s %4d rows -> %s", stem, len(frame), write_table(frame, out / f"{stem}.csv"))
    log.info(
        "%d artists, %d with a defined diversity (>= %d labelled neighbours); within-genre "
        "share of labelled weight %.1f %%",
        len(artists),
        len(scores),
        min_labelled,
        100 * summary["within_genre_weight_share"][0],
    )
    return tables


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
