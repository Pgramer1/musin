"""Tests for musin.analytics.bridges and bridge_figures (F4.3) on tiny hand-made graphs."""

import pandas as pd
import pytest

from musin.analytics.bridge_figures import CATEGORICAL, OTHER, UNLABELLED, ego_layout
from musin.analytics.bridges import (
    CYPHER_FILES,
    collab_pairs,
    diversity,
    genre_matrix,
    genre_profile,
    neighbour_genres,
    percentile_rank,
    square_matrix,
)
from musin.gds.setup import Session

EDGES = "src_id long, dst_id long, edge_type string, weight long"
NODES = "artist_id long, primary_genre string"


@pytest.fixture
def graph(spark):
    # Artist 1 collaborates with 2 (rock, w 3 via two types), 3 (jazz, w 1), 4 (unlabelled, w 5)
    # and 5 (rock, w 1); 2 and 3 also collaborate (w 2).
    edges = spark.createDataFrame(
        [
            (1, 2, "CO_CREDITED", 2),
            (2, 1, "PERFORMED_FOR", 1),
            (1, 3, "FEATURED", 1),
            (1, 4, "CO_CREDITED", 5),
            (5, 1, "PRODUCED_FOR", 1),
            (2, 3, "CO_CREDITED", 2),
        ],
        EDGES,
    )
    nodes = spark.createDataFrame(
        [(1, "pop"), (2, "rock"), (3, "jazz"), (4, None), (5, "rock")], NODES
    )
    return edges, nodes


def test_collab_pairs_merge_types_and_directions(graph):
    edges, _ = graph
    pairs = {(r.a, r.b): r.w for r in collab_pairs(edges).collect()}
    assert pairs == {(1, 2): 3, (1, 3): 1, (1, 4): 5, (1, 5): 1, (2, 3): 2}


def test_diversity_formula_and_threshold(graph):
    edges, nodes = graph
    by_genre = neighbour_genres(collab_pairs(edges), nodes)
    d = {r.artist_id: r for r in diversity(by_genre, min_labelled=3).collect()}
    a1 = d[1]
    # labelled neighbours 2, 3, 5 (4 is unlabelled): rock 3 + 1 = 4, jazz 1 -> shares 0.8, 0.2
    assert (a1.labelled_neighbours, a1.labelled_weight, a1.neighbour_genres) == (3, 5, 2)
    assert a1.genre_diversity == pytest.approx(1 - 0.8**2 - 0.2**2)
    # artist 2 has 2 labelled neighbours (1 pop, 3 jazz) < 3 -> undefined
    assert d[2].labelled_neighbours == 2 and d[2].genre_diversity is None
    # artist 4's only neighbour is labelled: one genre, below threshold
    assert d[4].genre_diversity is None


def test_diversity_zero_for_single_genre(graph):
    edges, nodes = graph
    by_genre = neighbour_genres(collab_pairs(edges), nodes)
    d = {r.artist_id: r for r in diversity(by_genre, min_labelled=1).collect()}
    assert d[5].genre_diversity == pytest.approx(0.0)  # only neighbour: pop


def test_genre_matrix_symmetric_cells_and_unlabelled_excluded(graph):
    edges, nodes = graph
    cells = {
        (r.genre_a, r.genre_b): r.weight for r in genre_matrix(collab_pairs(edges), nodes).collect()
    }
    # pairs: 1-2 pop/rock 3, 1-3 pop/jazz 1, 1-5 pop/rock 1, 2-3 rock/jazz 2; 1-4 unlabelled
    assert cells == {("pop", "rock"): 4, ("jazz", "pop"): 1, ("jazz", "rock"): 2}


def test_square_matrix_symmetric_with_zeros():
    cells = pd.DataFrame(
        {
            "genre_a": ["jazz", "rock"],
            "genre_b": ["rock", "rock"],
            "weight": [2, 6],
            "pairs": [1, 3],
        }
    )
    m = square_matrix(cells, ["jazz", "rock"]).set_index(["genre_a", "genre_b"])
    assert m.loc[("jazz", "rock"), "weight"] == m.loc[("rock", "jazz"), "weight"] == 2
    assert m.loc[("jazz", "jazz"), "weight"] == 0
    assert m.loc[("rock", "rock"), "share"] == pytest.approx(6 / 8)


def test_percentile_rank_mid_rank_for_ties():
    pct = percentile_rank(pd.Series([0.0, 0.0, 5.0, 9.0]))
    # zeros: 0 below, 2 equal -> (0 + 1) / 4; 5: 2 below -> (2 + 0.5) / 4; 9: (3 + 0.5) / 4
    assert pct.tolist() == pytest.approx([0.25, 0.25, 0.625, 0.875])


def test_genre_profile():
    rows = pd.DataFrame({"genre": ["jazz", "rock", "pop", "folk"], "w": [1, 6, 2, 1]})
    assert genre_profile(rows, 3) == "rock 60%, pop 20%, folk 10%"  # tie jazz/folk: alphabetical


def test_ego_layout_groups_and_caps():
    nb = pd.DataFrame(
        {
            "artistId": [1, 2, 3, 4, 5, 6, 7],
            "w": [9, 8, 7, 6, 5, 4, 1],
            "name": list("ABCDEFG"),
            "primaryGenre": ["rock", "jazz", "pop", "folk", None, "rock", "metal"],
        }
    )
    shown, legend = ego_layout(nb, max_nb=6)
    assert len(shown) == 6 and 7 not in set(shown["artistId"])  # weakest dropped
    assert legend == [
        ("rock", CATEGORICAL[0]),
        ("jazz", CATEGORICAL[1]),
        ("pop", CATEGORICAL[2]),
        ("other genres", OTHER),
        ("unlabelled", UNLABELLED),
    ]
    assert list(shown["group"]) == ["rock", "rock", "jazz", "pop", "other genres", "unlabelled"]


def test_bridges_statements_exist():
    s = Session(driver=None, files=CYPHER_FILES)
    for name in ("betweenness", "write_bridges", "count_written"):
        assert f"bridges.{name}" in s.queries
