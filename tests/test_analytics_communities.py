"""Tests for musin.analytics.communities (F4.2) and the loaded-graph guard, without Neo4j."""

import json

import pandas as pd
import pytest

from musin.analytics.common import check_still_loaded
from musin.analytics.communities import (
    CYPHER_FILES,
    genre_agreement,
    largest_component,
    relabel_by_size,
    size_bucket,
    size_distribution,
    stability,
    top_communities,
)
from musin.gds.setup import Session


def test_relabel_by_size_largest_first_ties_by_smallest_artist():
    assign = pd.DataFrame(
        {"artistId": [10, 11, 12, 5, 6, 1, 2], "community": [77, 77, 77, 3, 3, 9, 9]}
    )
    out = relabel_by_size(assign).set_index("artistId")["community"]
    # 77 has 3 members -> 1; 3 and 9 both have 2: 9 holds artist 1 (< 5) -> 2, then 3 -> 3.
    assert out.to_dict() == {10: 1, 11: 1, 12: 1, 5: 3, 6: 3, 1: 2, 2: 2}


@pytest.mark.parametrize(
    ("size", "bucket"),
    [
        (1, "1"),
        (2, "2-9"),
        (9, "2-9"),
        (10, "10-99"),
        (99, "10-99"),
        (100, "100-999"),
        (25_000, "10000-99999"),
    ],
)
def test_size_bucket(size, bucket):
    assert size_bucket(size) == bucket


def test_size_distribution():
    # communities of size 1, 2, 3 and 12
    assign = pd.DataFrame({"community": [1] * 12 + [2] * 3 + [3] * 2 + [4]})
    out = size_distribution(assign.assign(artistId=range(len(assign))))
    assert list(out["bucket"]) == ["1", "2-9", "10-99"]
    assert list(out["communities"]) == [1, 2, 1]
    assert list(out["artists"]) == [1, 5, 12]
    assert out["artist_share"].sum() == pytest.approx(1.0)


def test_genre_agreement_ignores_unlabelled():
    frame = pd.DataFrame(
        {
            "community": [1, 1, 2, 2, 3],
            "primaryGenre": ["rock", "rock", "jazz", "jazz", None],
        }
    )
    out = genre_agreement(frame)
    assert out["labelled_artists"] == 4
    assert out["nmi_genre"] == pytest.approx(1.0)
    assert out["ami_genre"] == pytest.approx(1.0)


def test_genre_agreement_independent_is_zero():
    frame = pd.DataFrame(
        {"community": [1, 1, 2, 2], "primaryGenre": ["rock", "jazz", "rock", "jazz"]}
    )
    assert genre_agreement(frame)["nmi_genre"] == pytest.approx(0.0, abs=1e-12)


def test_stability_aligns_on_artist_id():
    a = pd.Series([1, 1, 2, 2], index=[10, 11, 12, 13])
    b = pd.Series([5, 5, 6, 6], index=[13, 12, 11, 10])  # same partition, other order and ids
    rows = stability({"record": a, "stability_1": b}, lcc=pd.Index([10, 11, 12]))
    assert rows == [
        {
            "run_a": "record",
            "run_b": "stability_1",
            "artists": 4,
            "nmi": 1.0,
            "lcc_artists": 3,
            "nmi_lcc": 1.0,
        }
    ]


def test_largest_component_ties_by_smallest_artist():
    comp = pd.Series(
        [7, 7, 7, 3, 3, 3, 9],
        index=pd.Index([20, 21, 22, 5, 6, 30, 1], name="artistId"),
        name="component",
    )
    # components 7 and 3 both hold 3 artists; 3 holds artist 5 (< 20)
    assert sorted(largest_component(comp)) == [5, 6, 30]


def test_top_communities_composition_and_examples():
    assign = pd.DataFrame(
        {
            "artistId": [1, 2, 3, 4, 5, 6],
            "community": [1, 1, 1, 1, 2, 3],
            "name": ["A", "B", "C", "D", "E", "F"],
            "primaryGenre": ["rock", "rock", "jazz", None, "pop", "pop"],
            "degreeWeighted": [5.0, 9.0, 9.0, 1.0, 2.0, 3.0],
        }
    )
    out = top_communities(assign, ["jazz", "pop", "rock"], k=2, examples=2)
    assert list(out["community"]) == [1, 2]
    first = out.iloc[0]
    assert (first["size"], first["labelled"]) == (4, 3)
    assert first["labelled_share"] == pytest.approx(0.75)
    assert first["top_genre"] == "rock"
    assert first["share_rock"] == pytest.approx(2 / 3)
    assert first["share_jazz"] == pytest.approx(1 / 3)
    assert first["share_pop"] == 0.0
    assert first["example_artists"] == "B; C"  # tie at 9.0: lower artistId first


def test_communities_statements_exist():
    s = Session(driver=None, files=CYPHER_FILES)
    for name in ("estimate_louvain", "louvain", "wcc", "genres", "write_louvain", "count_written"):
        assert f"communities.{name}" in s.queries
    assert "centrality.degree" in s.queries and "centrality.stream_property" in s.queries
    assert "relationshipWeightProperty: 'weight'" in s.queries["communities.louvain"]


class FakeCfg:
    def __init__(self, root):
        self.data_root = root


def test_check_still_loaded(tmp_path):
    cfg = FakeCfg(tmp_path)
    marker = tmp_path / "neo4j" / "loaded_graph.json"
    marker.parent.mkdir()
    loaded = {"graph": "sample-f0.05", "graph_fingerprints": {"edges": "1"}}
    marker.write_text(json.dumps(loaded))
    check_still_loaded(cfg, loaded)
    marker.write_text(json.dumps({"graph": "full", "graph_fingerprints": {"edges": "2"}}))
    with pytest.raises(RuntimeError, match="changed"):
        check_still_loaded(cfg, loaded)
    marker.unlink()
    with pytest.raises(RuntimeError, match="missing"):
        check_still_loaded(cfg, loaded)
