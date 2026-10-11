"""Tests for musin.analytics.centrality (F4.1) and analytics.common, without Neo4j."""

import pandas as pd
import pytest

from musin.analytics.centrality import (
    CYPHER_FILES,
    MEASURES,
    agreement,
    cross_projection,
    jaccard,
    spearman,
    top_k,
)
from musin.analytics.common import loaded_graph, results_dir
from musin.common.config import ConfigError
from musin.gds.setup import Session


def scores(rows):
    return pd.DataFrame(rows, columns=["artistId", "degreeWeighted", "pagerank", "betweenness"])


TOY = scores(
    [
        (1, 10.0, 0.5, 0.0),
        (2, 8.0, 0.9, 30.0),
        (3, 8.0, 0.1, 10.0),
        (4, 1.0, 0.2, 0.0),
    ]
)


def test_top_k_orders_by_value_then_artist_id():
    top = top_k(TOY, "degreeWeighted", 3)
    assert list(top["artistId"]) == [1, 2, 3]  # 2 and 3 tie at 8.0: lower id first
    assert list(top["rank"]) == [1, 2, 3]


def test_top_k_shorter_than_k():
    assert len(top_k(TOY, "pagerank", 10)) == 4


def test_jaccard():
    assert jaccard({1, 2, 3}, {2, 3, 4}) == 0.5
    assert jaccard(set(), set()) == 1.0
    assert jaccard({1}, {2}) == 0.0


def test_spearman_identical_and_reversed():
    x = pd.Series([1.0, 2.0, 3.0, 4.0])
    assert spearman(x, x * 10) == pytest.approx(1.0)
    assert spearman(x, -x) == pytest.approx(-1.0)


def test_spearman_uses_average_ranks_for_ties():
    # x ranks (1, 2.5, 2.5, 4); y ranks (1, 2, 3, 4): Pearson of the ranks.
    x = pd.Series([0.0, 5.0, 5.0, 9.0])
    y = pd.Series([1.0, 2.0, 3.0, 4.0])
    expected = pd.Series([1, 2.5, 2.5, 4]).corr(pd.Series([1, 2, 3, 4]))
    assert spearman(x, y) == pytest.approx(expected)


def test_agreement_has_one_row_per_pair():
    rows = agreement(TOY, list(MEASURES), k=2)
    pairs = [(r["measure_a"], r["measure_b"]) for r in rows]
    assert pairs == [
        ("degreeWeighted", "pagerank"),
        ("degreeWeighted", "betweenness"),
        ("pagerank", "betweenness"),
    ]
    by_pair = {(r["measure_a"], r["measure_b"]): r for r in rows}
    # top-2 degree {1, 2}, top-2 pagerank {2, 1}, top-2 betweenness {2, 3}
    assert by_pair[("degreeWeighted", "pagerank")]["jaccard_top_k"] == 1.0
    assert by_pair[("degreeWeighted", "betweenness")]["jaccard_top_k"] == pytest.approx(1 / 3)
    assert all(r["n_artists"] == 4 and r["top_k"] == 2 for r in rows)


def test_cross_projection_uses_common_artists_for_rho_and_own_lists_for_jaccard():
    second = scores([(2, 3.0, 0.9, 30.0), (3, 3.0, 0.1, 10.0), (4, 1.0, 0.2, 0.0)])
    rows = {r["measure_a"]: r for r in cross_projection(TOY, second, list(MEASURES), k=1)}
    assert rows["degreeWeighted"]["n_artists"] == 3  # artist 1 is only in the first
    # On artists 2, 3, 4 the degree order is the same in both projections ...
    assert rows["degreeWeighted"]["spearman_rho"] == pytest.approx(1.0)
    # ... but the first projection's top-1 is artist 1, the second's is artist 2.
    assert rows["degreeWeighted"]["jaccard_top_k"] == 0.0


class FakeCfg:
    def __init__(self, tmp_path):
        self.data_root = tmp_path / "data"
        self.source = tmp_path / "repo" / "config" / "pipeline.yaml"

    def get(self, key):
        assert key == "reports_dir"
        return "reports/"


def test_results_dir_full_and_sample(tmp_path):
    cfg = FakeCfg(tmp_path)
    assert results_dir(cfg, "full") == (tmp_path / "repo" / "reports" / "tables").resolve()
    assert results_dir(cfg, "sample-f0.05") == (
        tmp_path / "data" / "sample" / "f0.05" / "reports" / "tables"
    )
    with pytest.raises(ConfigError):
        results_dir(cfg, "toy")


def test_loaded_graph_refuses_missing_marker_and_toy(tmp_path):
    cfg = FakeCfg(tmp_path)
    with pytest.raises(ConfigError, match="missing"):
        loaded_graph(cfg)
    marker = cfg.data_root / "neo4j" / "loaded_graph.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"graph": "toy"}')
    with pytest.raises(ConfigError, match="toy"):
        loaded_graph(cfg)


def test_centrality_statements_exist():
    s = Session(driver=None, files=CYPHER_FILES)
    for alg in MEASURES.values():
        assert f"centrality.{alg}" in s.queries
        assert f"centrality.estimate_{alg}" in s.queries
    for name in ("stream_property", "artists", "write_back", "count_written"):
        assert f"centrality.{name}" in s.queries
    assert "projections.weight_total" in s.queries


def test_betweenness_is_unweighted():
    s = Session(driver=None, files=CYPHER_FILES)
    for name in ("centrality.betweenness", "centrality.estimate_betweenness"):
        code = "\n".join(
            line for line in s.queries[name].splitlines() if not line.strip().startswith("//")
        )
        assert "relationshipWeightProperty" not in code
        # D-033: betweenness has its own (single-threaded) concurrency setting.
        assert "concurrency: $betweennessConcurrency" in code
