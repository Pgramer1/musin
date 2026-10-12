"""Tests for musin.analytics.validation (F4.6), without Neo4j."""

from pathlib import Path

import pandas as pd
import pytest

from musin.analytics.validation import CYPHER_FILES, MEASURES, ranks, summarise
from musin.gds.setup import Session

REPO = Path(__file__).resolve().parents[1]


def test_ranks_defined_only_ties_by_artist_id():
    scores = pd.DataFrame({"artistId": [4, 2, 3, 1], "x": [5.0, None, 5.0, 1.0]})
    r = ranks(scores, "x").set_index("artistId")
    assert 2 not in r.index  # undefined
    assert r.loc[3, "x_rank"] == 1 and r.loc[4, "x_rank"] == 2 and r.loc[1, "x_rank"] == 3
    assert r["x_pct"].tolist() == pytest.approx([1.0, 2 / 3, 1 / 3])


def test_summarise_counts_top_k():
    table = pd.DataFrame(
        {
            "in_graph": [True, True, False],
            **{f"{m}_rank": [5, 300, None] for m in MEASURES},
            **{f"{m}_pct": [0.99, 0.5, None] for m in MEASURES},
        }
    )
    out = summarise(table, {m: 1000 for m in MEASURES}).set_index("measure")
    row = out.loc["bridgeScore"]
    assert (row["listed"], row["listed_in_graph"], row["listed_defined"]) == (3, 2, 2)
    assert (row["in_top_10"], row["in_top_25"], row["in_top_100"], row["in_top_1000"]) == (
        1,
        1,
        1,
        2,
    )
    assert row["median_rank"] == 152.5


def test_committed_validation_list():
    listed = pd.read_csv(REPO / "config" / "validation_artists.csv")
    assert len(listed) == 15
    assert listed["mbid"].is_unique
    assert listed["mbid"].str.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}").all()


def test_validation_statement_returns_every_measure():
    q = Session(driver=None, files=CYPHER_FILES).queries["validation.scores"]
    for m in MEASURES:
        assert f"AS {m}" in q
