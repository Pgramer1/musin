"""Tests for musin.graph.checks (F3.3): a valid tiny graph passes, each corruption is caught."""

import pytest

from musin.graph.build import CO_CREDITED, FEATURED, PERFORMED_FOR, PRODUCED_FOR
from musin.graph.checks import (
    check_decades,
    check_edges,
    check_genres,
    check_nodes,
    check_schema,
    row_changes,
)

NODES = (
    "artist_id long, mbid string, name string, artist_type string, area_name string, "
    "begin_year int, recording_count long, primary_genre string, genre_votes long"
)
EDGES = "src_id long, dst_id long, edge_type string, weight long, first_year int, last_year int"
DECADES = "src_id long, dst_id long, edge_type string, decade int, weight long"
GENRES = "artist_id long, parent_genre string, share double"
PARENTS = {"rock", "jazz", "blues"}

VALID = {
    "nodes": [
        (1, "m1", "A", "Person", None, 1970, 3, "rock", 5),
        (2, "m2", "B", "Group", "Oslo", None, 2, "blues", 4),  # tied shares: blues < jazz
        (3, "m3", "C", None, None, None, 1, None, None),
    ],
    "edges": [
        (1, 2, CO_CREDITED, 3, 1995, 2003),  # one of the 3 recordings has no year
        (1, 3, FEATURED, 1, None, None),
        (3, 2, PERFORMED_FOR, 2, 2001, 2001),
    ],
    "decades": [
        (1, 2, CO_CREDITED, 1990, 1),
        (1, 2, CO_CREDITED, 2000, 1),
        (3, 2, PERFORMED_FOR, 2000, 2),
    ],
    "genres": [(1, "rock", 0.6), (1, "jazz", 0.4), (2, "jazz", 0.5), (2, "blues", 0.5)],
}


def run_all(spark, **changes):
    g = {**VALID, **changes}
    nodes = spark.createDataFrame(g["nodes"], NODES)
    edges = spark.createDataFrame(g["edges"], EDGES)
    decades = spark.createDataFrame(g["decades"], DECADES)
    genres = spark.createDataFrame(g["genres"], GENRES)
    checks = check_nodes(nodes, edges, 1, ["Character"], PARENTS)
    checks += check_edges(edges, nodes, 1877, 2026)
    checks += check_decades(decades, edges)
    checks += check_genres(genres, nodes, PARENTS)
    return {f"{c.table}.{c.name}": c.violations for c in checks}


def failing(result):
    return {k: v for k, v in result.items() if v}


def test_valid_graph_passes_everything(spark):
    assert failing(run_all(spark)) == {}


def test_schema_against_contract(spark):
    good = spark.createDataFrame([], GENRES)
    assert check_schema("artist_genre", good).violations == 0
    wrong_type = spark.createDataFrame([], "artist_id int, parent_genre string, share double")
    assert check_schema("artist_genre", wrong_type).violations == 1
    wrong_order = spark.createDataFrame([], "parent_genre string, artist_id long, share double")
    assert check_schema("artist_genre", wrong_order).violations == 1


@pytest.mark.parametrize(
    ("extra_edge", "expected"),
    [
        ((2, 2, PRODUCED_FOR, 1, None, None), {"edges.self_loop": 1}),
        ((2, 1, CO_CREDITED, 1, None, None), {"edges.co_credited_not_ordered": 1}),
        ((1, 2, CO_CREDITED, 1, None, None), {"edges.duplicate_key": 1}),
        ((1, 2, "KNOWS", 1, None, None), {"edges.unknown_edge_type": 1}),
        ((1, 3, PRODUCED_FOR, 0, None, None), {"edges.weight_below_1": 1}),
        ((1, 3, PRODUCED_FOR, 1, 2005, None), {"edges.year_null_mismatch": 1}),
        ((1, 3, PRODUCED_FOR, 1, 2005, 2001), {"edges.first_year_after_last": 1}),
        ((1, 3, PRODUCED_FOR, 1, 1850, 1850), {"edges.year_out_of_range": 1}),
        ((1, 9, PRODUCED_FOR, 1, None, None), {"edges.endpoint_not_a_node": 1}),
    ],
)
def test_edge_corruptions(spark, extra_edge, expected):
    result = run_all(spark, edges=VALID["edges"] + [extra_edge])
    # a dated extra edge also lacks decade rows; only the edge checks are asserted here
    assert {k: v for k, v in failing(result).items() if k.startswith("edges.")} == expected


def test_node_corruptions(spark):
    nodes = VALID["nodes"] + [
        (4, "m4", "D", "Character", None, None, 0, "polka", None),  # isolated, 3 problems + votes
        (1, "m1", "A", "Person", None, 1970, 3, "rock", 5),  # duplicate id and mbid
    ]
    got = failing(run_all(spark, nodes=nodes))
    assert got["nodes_artist.node_without_edge"] == 1
    assert got["nodes_artist.below_min_recordings"] == 1
    assert got["nodes_artist.excluded_artist_type"] == 1
    assert got["nodes_artist.unknown_primary_genre"] == 1
    assert got["nodes_artist.genre_votes_mismatch"] == 1
    assert got["nodes_artist.duplicate_artist_id"] == 1
    assert got["nodes_artist.duplicate_mbid"] == 1


def test_decade_corruptions(spark):
    decades = [
        (1, 2, CO_CREDITED, 1990, 2),  # 2 + 1 + 1 > edge weight 3: invariant
        (1, 2, CO_CREDITED, 2000, 1),
        (1, 2, CO_CREDITED, 2010, 1),  # after last_year 2003
        (1, 3, FEATURED, 1990, 1),  # edge has no known year
        (5, 6, CO_CREDITED, 1995, 1),  # no such edge, and not a decade start
    ]  # PERFORMED_FOR 3 -> 2 (dated) has no rows left
    got = failing(run_all(spark, decades=decades))
    assert got == {
        "edges_by_decade.invariant_decades_exceed_weight": 1,
        "edges_by_decade.decade_outside_edge_years": 2,
        "edges_by_decade.last_decade_mismatch": 1,
        "edges_by_decade.row_without_edge": 1,
        "edges_by_decade.decade_not_multiple_of_10": 1,
        "edges_by_decade.dated_edge_without_decades": 1,
    }


def test_genre_corruptions(spark):
    genres = [
        (1, "rock", 0.6),
        (1, "jazz", 0.3),  # sums to 0.9
        (2, "jazz", 0.5),
        (2, "blues", 0.5),
        (3, "rock", 1.0),  # node 3 has no primary genre
        (7, "polka", 1.0),  # not a node, unknown parent
    ]
    got = failing(run_all(spark, genres=genres))
    assert got == {
        "artist_genre.shares_not_summing_to_1": 1,
        "artist_genre.unknown_parent_genre": 1,
        "artist_genre.artist_not_a_node": 1,
        "nodes_artist.primary_genre_not_top_share": 1,
    }


def test_tie_break_must_be_alphabetical(spark):
    nodes = [
        VALID["nodes"][0],
        (2, "m2", "B", "Group", "Oslo", None, 2, "jazz", 4),
        VALID["nodes"][2],
    ]
    got = failing(run_all(spark, nodes=nodes))
    assert got == {"nodes_artist.primary_genre_not_top_share": 1}


def test_row_changes():
    base = {"edges": 1000, "nodes_artist": 100, "edges_FEATURED": 0}
    now = {"edges": 1040, "nodes_artist": 80, "edges_FEATURED": 0, "edges_NEW": 5}
    got = {c.name: c for c in row_changes(now, base, 0.05, accept=False)}
    assert got["edges"].violations == 0  # +4 %
    assert got["nodes_artist"].violations == 1 and not got["nodes_artist"].passed  # -20 %
    assert got["edges_FEATURED"].violations == 0
    assert got["edges_NEW"].violations == 0 and "no previous" in got["edges_NEW"].detail
    accepted = {c.name: c for c in row_changes(now, base, 0.05, accept=True)}
    assert accepted["nodes_artist"].passed and accepted["nodes_artist"].status == "accepted"
    first = row_changes(now, None, 0.05, accept=False)
    assert all(c.passed for c in first)
