"""Tests for musin.analytics.trends (F4.4) on tiny hand-made graphs."""

import math

import pandas as pd
import pytest

from musin.analytics.trends import complete_decades, decade_shares

BY_DECADE = "src_id long, dst_id long, edge_type string, decade int, weight long"
NODES = "artist_id long, primary_genre string"


@pytest.fixture
def frames(spark):
    nodes = spark.createDataFrame([(1, "rock"), (2, "rock"), (3, "jazz"), (4, None)], NODES)
    by_decade = spark.createDataFrame(
        [
            # 1970s: rock-rock 4 (labelled, same), rock-jazz 1 (labelled, cross),
            #        rock-unlabelled 5 (unlabelled) -> cross share 1/5
            (1, 2, "CO_CREDITED", 1970, 4),
            (1, 3, "FEATURED", 1970, 1),
            (1, 4, "CO_CREDITED", 1970, 5),
            # 1980s: only a producer edge, cross-genre -> featuring share undefined
            (3, 2, "PRODUCED_FOR", 1980, 2),
            # 1950s: outside the range, ignored
            (1, 3, "CO_CREDITED", 1950, 100),
        ],
        BY_DECADE,
    )
    return by_decade, nodes


def test_decade_shares(frames):
    by_decade, nodes = frames
    out = decade_shares(by_decade, nodes, 1960, 2020).toPandas().set_index("decade")
    assert list(out.index) == [1970, 1980]
    d70 = out.loc[1970]
    assert (d70["edges"], d70["total_weight"], d70["labelled_weight"], d70["cross_weight"]) == (
        3,
        10,
        5,
        1,
    )
    assert d70["labelled_share"] == pytest.approx(0.5)
    assert d70["cross_genre_share"] == pytest.approx(0.2)
    # featuring share uses all edges, labelled or not: 1 / (1 + 4 + 5)
    assert (d70["featured_weight"], d70["co_credited_weight"]) == (1, 9)
    assert d70["featuring_share"] == pytest.approx(0.1)
    d80 = out.loc[1980]
    assert d80["cross_genre_share"] == pytest.approx(1.0)
    assert d80["produced_for_weight"] == 2
    assert d80["featuring_share"] is None or math.isnan(d80["featuring_share"])


def test_decade_shares_counts_each_rows_weight_once(frames):
    by_decade, nodes = frames
    out = decade_shares(by_decade, nodes, 1960, 2020).toPandas()
    type_cols = [
        "co_credited_weight",
        "featured_weight",
        "produced_for_weight",
        "performed_for_weight",
    ]
    assert (out[type_cols].sum(axis=1) == out["total_weight"]).all()


def test_complete_decades_fills_empty_decades():
    frame = pd.DataFrame(
        {
            "decade": [1970],
            "edges": [3],
            "total_weight": [10],
            "labelled_weight": [5],
            "cross_weight": [1],
            "co_credited_weight": [9],
            "featured_weight": [1],
            "produced_for_weight": [0],
            "performed_for_weight": [0],
            "labelled_share": [0.5],
            "cross_genre_share": [0.2],
            "featuring_share": [0.1],
        }
    )
    out = complete_decades(frame, 1960, 1990)
    assert list(out["decade"]) == [1960, 1970, 1980, 1990]
    assert list(out["total_weight"]) == [0, 10, 0, 0]
    assert out["cross_genre_share"].isna().tolist() == [True, False, True, True]
