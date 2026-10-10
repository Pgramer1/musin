"""Tests for musin.common.cypher and the F1.5 reference implementations."""

from pathlib import Path

import pytest

from musin.common.cypher import load_queries, parse_queries
from musin.neo4j_smoke import reference as ref

REPO = Path(__file__).resolve().parents[1]
TOY = REPO / "tests" / "fixtures" / "neo4j_toy"
EDGE_FILES = ("co_credited", "featured", "produced_for", "performed_for")
INT_GROUPS = frozenset({"Artist"})


# --- named Cypher statements ------------------------------------------------------


def test_parse_queries_names_and_strips_semicolon():
    text = (
        "// header comment\n\n// @name a\nRETURN 1;\n\n// @name b\n// why\nMATCH (n)\nRETURN n;\n"
    )
    q = parse_queries(text)
    assert list(q) == ["a", "b"]
    assert q["a"] == "RETURN 1"
    assert q["b"] == "// why\nMATCH (n)\nRETURN n"


@pytest.mark.parametrize(
    "text, message",
    [
        ("RETURN 1;", "before the first"),
        ("// @name a\nRETURN 1;\n// @name a\nRETURN 2;", "duplicate"),
        ("// @name a\nRETURN 1", "does not end"),
        ("// @name a\n// only a comment\n;", "empty"),
    ],
)
def test_parse_queries_rejects_malformed_files(text, message):
    with pytest.raises(ValueError, match=message):
        parse_queries(text)


def test_smoke_cypher_files_parse():
    names = {}
    for path in sorted((REPO / "neo4j" / "smoke").glob("*.cypher")):
        q = load_queries(path)
        assert not names.keys() & q.keys(), f"duplicate name across files: {path}"
        names.update(q)
    for needed in ("collab_undirected", "pagerank", "betweenness", "louvain", "drop_all"):
        assert needed in names


# --- import CSV reader -------------------------------------------------------------


def test_read_import_csv_types_nulls_and_quotes():
    artists = {a["artistId"]: a for a in ref.read_import_csv(TOY / "artists.csv", INT_GROUPS)}
    assert len(artists) == 12 and all(isinstance(k, int) for k in artists)
    assert artists[105]["areaName"] is None and artists[105]["beginYear"] is None
    assert artists[101]["beginYear"] == 1970 and artists[101]["recordingCount"] == 40
    assert artists[106]["name"] == 'Foxtrot "The Bridge"'
    assert artists[110]["name"] == "Juliet Rap, Jr."
    assert artists[101]["LABEL"] == "Artist"

    genres = ref.read_import_csv(TOY / "genres.csv", INT_GROUPS)
    assert {g["name"] for g in genres} == {"rock", "hip hop", "electronic"}

    edge = ref.read_import_csv(TOY / "edges_co_credited.csv", INT_GROUPS)[2]
    assert (edge["START_ID"], edge["END_ID"], edge["weight"]) == (101, 105, 1)
    assert edge["firstYear"] is None and edge["TYPE"] == "CO_CREDITED"


# --- reference algorithms ----------------------------------------------------------


def test_merged_pairs_sums_types_and_directions():
    merged = ref.merged_pairs([(1, 2, 3), (2, 1, 1), (2, 3, 2)])
    assert merged == {(1, 2): 4, (2, 3): 2}
    with pytest.raises(ValueError, match="self-loop"):
        ref.merged_pairs([(1, 1, 1)])


def test_weighted_degree():
    assert ref.weighted_degree({(1, 2): 4, (2, 3): 2}) == {1: 4, 2: 6, 3: 2}


def test_pagerank_path_graph_closed_form():
    # a-b-c, unweighted: a = c = 0.21375 / 0.2775, b = 0.15 + 1.7 a; scores sum to n.
    pr = ref.pagerank({(1, 2): 1.0, (2, 3): 1.0}, damping=0.85, tolerance=1e-12)
    a = 0.21375 / 0.2775
    assert pr[1] == pytest.approx(a) and pr[3] == pytest.approx(a)
    assert pr[2] == pytest.approx(0.15 + 1.7 * a)
    assert sum(pr.values()) == pytest.approx(3)


def test_pagerank_uses_weights():
    # Hub 2 with a heavy edge to 1 and a light edge to 3: 1 gets more of 2's score.
    pr = ref.pagerank({(1, 2): 9.0, (2, 3): 1.0}, damping=0.85, tolerance=1e-12)
    assert pr[1] > pr[3]


def test_betweenness_star_and_path():
    star = ref.betweenness({(0, i): 1 for i in range(1, 5)})
    assert star[0] == pytest.approx(6)  # C(4, 2) leaf pairs all pass the centre
    assert all(star[i] == 0 for i in range(1, 5))
    path = ref.betweenness({(1, 2): 1, (2, 3): 1})
    assert path == {1: 0, 2: 1, 3: 0}


def test_betweenness_counts_parallel_edges():
    # Square 1-2-3-4-1. 1 and 3 are joined via 2 and via 4; doubling 1-2
    # makes 2 of the 3 shortest 1..3 paths run through 2.
    square = {(1, 2): 1, (2, 3): 1, (3, 4): 1, (1, 4): 1}
    assert ref.betweenness(square)[2] == pytest.approx(0.5)
    assert ref.betweenness({**square, (1, 2): 2})[2] == pytest.approx(2 / 3)


def test_modularity_two_triangles():
    pairs = {(1, 2): 1, (1, 3): 1, (2, 3): 1, (4, 5): 1, (4, 6): 1, (5, 6): 1}
    split = {1: 0, 2: 0, 3: 0, 4: 1, 5: 1, 6: 1}
    assert ref.modularity(pairs, split) == pytest.approx(0.5)
    assert ref.modularity(pairs, dict.fromkeys(split, 0)) == pytest.approx(0)


def test_toy_graph_expected_shape():
    edges = [r for k in EDGE_FILES for r in ref.read_import_csv(TOY / f"edges_{k}.csv", INT_GROUPS)]
    pairs = ref.merged_pairs([(e["START_ID"], e["END_ID"], e["weight"]) for e in edges])
    assert len(edges) == 22 and len(pairs) == 19
    assert pairs[(108, 109)] == 7  # CO_CREDITED 5 + FEATURED 2
    assert pairs[(101, 103)] == 4  # CO_CREDITED 3 + PERFORMED_FOR 1 (reverse direction)
    bt = ref.betweenness({p: 1 for p in pairs})
    assert max(bt, key=bt.get) == 106
