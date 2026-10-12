"""Tests for musin.analytics.separation (F4.5), without Neo4j."""

import pytest

from musin.analytics.separation import CYPHER_FILES, canonical, describe_hop, main, resolve
from musin.gds.setup import Session


def cand(artist_id, mbid, name="LSO", rc=10):
    return {
        "artistId": artist_id,
        "mbid": mbid,
        "name": name,
        "type": "Orchestra",
        "recordingCount": rc,
        "primaryGenre": "classical",
    }


def test_resolve_unique_name():
    assert resolve([cand(1, "m1")], "LSO")["artistId"] == 1


def test_resolve_mbid_wins_over_names():
    # a name equal to another artist's MBID is absurd, but an MBID match must win
    rows = [cand(1, "m1"), cand(2, "m2", name="m1")]
    assert resolve(rows, "m1")["artistId"] == 1


def test_resolve_refuses_ambiguous_name_and_lists_candidates():
    with pytest.raises(RuntimeError, match="2 artists are named 'LSO'.*m1.*m2"):
        resolve([cand(1, "m1", rc=50), cand(2, "m2")], "LSO")


def test_resolve_missing():
    with pytest.raises(RuntimeError, match="no artist"):
        resolve([], "Nobody")


def test_canonical_dedupes_parallel_relationships_and_picks_smallest():
    seqs = [[1, 9, 5], [1, 3, 5], [1, 9, 5], [1, 3, 5]]  # two relationship paths per sequence
    assert canonical(seqs) == ([1, 3, 5], 2)


def test_canonical_no_path():
    assert canonical([]) == (None, 0)


def test_describe_hop():
    rels = [
        {"type": "CO_CREDITED", "weight": 1, "forward": True},
        {"type": "FEATURED", "weight": 3, "forward": False},
    ]
    assert describe_hop("A", "B", rels) == "A -[CO_CREDITED 1, FEATURED← 3]- B"


def test_paths_never_use_genre_nodes():
    s = Session(driver=None, files=CYPHER_FILES)
    for name in ("shortest_paths", "hop_relations"):
        q = s.queries[f"separation.{name}"]
        assert ":CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR" in q
        assert "[*" not in q and "-[r]-" not in q
    assert "allShortestPaths" in s.queries["separation.shortest_paths"]


def test_cli_requires_a_pair():
    with pytest.raises(SystemExit):
        main(["--config", "config/pipeline.yaml"])
