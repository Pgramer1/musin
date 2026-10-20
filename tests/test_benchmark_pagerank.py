"""Tests for musin.benchmark.pagerank (F5.2): input pairs, comparison, summary, GraphFrames."""

from pathlib import Path

import pandas as pd
import pytest

from musin.benchmark.common import (
    LOADED_MARKER,
    collect,
    finished,
    read_loaded,
    run_file,
    settings,
    write_json,
)
from musin.benchmark.pagerank import compare, summary, undirected_edges
from musin.common.config import load_config

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"
EDGES = "src_id long, dst_id long, edge_type string, weight long"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "musin-data"
    monkeypatch.setenv("MUSIN_DATA", str(root))
    return root


def reference_pagerank(pairs: list[tuple[int, int]], damping: float, steps: int = 500) -> dict:
    """Unnormalised PageRank by power iteration on an undirected graph (GDS and GraphX form)."""
    nb: dict[int, list[int]] = {}
    for a, b in pairs:
        nb.setdefault(a, []).append(b)
        nb.setdefault(b, []).append(a)
    pr = {v: 1.0 for v in nb}
    for _ in range(steps):
        pr = {v: (1 - damping) + damping * sum(pr[u] / len(nb[u]) for u in nb[v]) for v in nb}
    return pr


def test_undirected_edges_collapse_types_and_directions(spark):
    edges = spark.createDataFrame(
        [
            (1, 2, "CO_CREDITED", 3),
            (2, 1, "FEATURED", 1),  # same pair, other direction and type
            (1, 2, "PERFORMED_FOR", 2),
            (3, 1, "PRODUCED_FOR", 1),
        ],
        EDGES,
    )
    out = sorted(tuple(r) for r in undirected_edges(edges).collect())
    assert out == [(1, 2), (1, 3), (2, 1), (3, 1)]


def test_settings_share_analysis_keys(data_root):
    cfg = load_config(REPO_CONFIG)
    opts = settings(cfg)
    assert opts["damping"] == cfg.get("analysis.pagerank.damping")
    assert opts["max_iterations"] == cfg.get("analysis.pagerank.max_iterations")
    assert opts["cores"] == cfg.get("analysis.concurrency")
    assert opts["tolerance"] == pytest.approx(1e-7)
    assert opts["repeats"] >= 1


def test_compare_identical_and_shifted():
    a = pd.DataFrame({"artistId": [1, 2, 3, 4], "score": [4.0, 3.0, 2.0, 1.0]})
    same = compare(a, a.copy(), k=2)
    assert same["spearman_rho"] == pytest.approx(1.0)
    assert same["jaccard_top_k"] == 1.0 and same["common_top_k"] == 2
    assert same["max_abs_diff"] == 0.0

    b = pd.DataFrame({"artistId": [1, 2, 3, 4], "score": [4.0, 1.0, 2.0, 3.0]})
    r = compare(a, b, k=2)
    assert r["common_top_k"] == 1  # {1, 2} vs {1, 4}
    assert r["jaccard_top_k"] == pytest.approx(1 / 3)
    assert r["max_abs_diff"] == 2.0
    assert r["max_rel_diff"] == pytest.approx(2.0)  # artist 4: |1 - 3| / 1


def test_compare_refuses_different_artist_sets():
    a = pd.DataFrame({"artistId": [1, 2], "score": [1.0, 2.0]})
    b = pd.DataFrame({"artistId": [1, 3], "score": [1.0, 2.0]})
    with pytest.raises(RuntimeError, match="artist sets differ"):
        compare(a, b, k=1)


def test_summary_medians_and_ratio():
    def run(engine, repeat, load, compute, output):
        return {
            "engine": engine, "repeat": repeat, "load_s": load, "compute_s": compute,
            "output_s": output, "total_s": load + compute + output,
        }  # fmt: skip

    runs = [
        run("gds", 1, 10, 5, 2), run("gds", 2, 8, 4, 2), run("gds", 3, 9, 6, 2),
        run("graphframes", 1, 20, 100, 4), run("graphframes", 2, 22, 80, 4),
        run("graphframes", 3, 21, 90, 4),
    ]  # fmt: skip
    gds, gf = summary(runs)
    assert gds["engine"] == "gds" and gds["load_median_s"] == 9 and gds["compute_median_s"] == 5
    assert gds["total_median_s"] == 17 and gds["total_min_s"] == 14
    assert gf["compute_median_s"] == 90 and gf["compute_vs_gds"] == 18.0
    assert gf["total_vs_gds"] == round(115 / 17, 2)


def test_graphframes_matches_reference_pagerank(spark):
    from graphframes import GraphFrame

    pairs = [(1, 2), (2, 3), (3, 1), (3, 4), (4, 5), (5, 6), (6, 4), (2, 5)]
    edges = spark.createDataFrame([(a, b, "CO_CREDITED", 1) for a, b in pairs], EDGES)
    vertices = spark.createDataFrame([(v,) for v in range(1, 7)], "id long")
    ranked = GraphFrame(vertices, undirected_edges(edges)).pageRank(resetProbability=0.15, tol=1e-9)
    got = {r["id"]: r["pagerank"] for r in ranked.vertices.collect()}
    want = reference_pagerank(pairs, 0.85)
    assert got.keys() == want.keys()
    for v in want:
        assert got[v] == pytest.approx(want[v], rel=1e-6)
    assert sum(got.values()) == pytest.approx(6.0, rel=1e-6)  # unnormalised: sums to N


def test_summary_ratios_for_every_engine():
    def run(engine, total):
        return {"engine": engine, "repeat": 1, "load_s": 1.0, "compute_s": total - 2.0,
                "output_s": 1.0, "total_s": total}  # fmt: skip

    rows = {
        r["engine"]: r
        for r in summary([run("gds", 10.0), run("cugraph", 5.0), run("graphframes", 100.0)])
    }
    assert [r for r in rows] == ["gds", "graphframes", "cugraph"]  # fixed engine order
    assert rows["cugraph"]["total_vs_gds"] == 0.5 and rows["graphframes"]["total_vs_gds"] == 10.0
    assert "total_vs_gds" not in rows["gds"]


def test_marker_name_matches_the_import_stage():
    from musin.export.neo4j_check import LOADED_MARKER as EXPORT_MARKER

    assert LOADED_MARKER == EXPORT_MARKER


def test_read_loaded(data_root):
    cfg = load_config(REPO_CONFIG)
    write_json(data_root / "neo4j" / LOADED_MARKER, {"graph": "full", "source": "/data/graph"})
    assert read_loaded(cfg)["graph"] == "full"
    write_json(data_root / "neo4j" / LOADED_MARKER, {"graph": "toy"})
    with pytest.raises(Exception, match="toy graph"):
        read_loaded(cfg)


def test_resume_skips_only_runs_of_the_same_graph(tmp_path):
    write_json(run_file(tmp_path, "cugraph", 1), {"repeat": 1, "edges_fingerprint": "-1"})
    write_json(run_file(tmp_path, "cugraph", 2), {"repeat": 2, "edges_fingerprint": "-2"})
    assert finished(tmp_path, "cugraph", 1, "-1") == {"repeat": 1, "edges_fingerprint": "-1"}
    assert finished(tmp_path, "cugraph", 2, "-1") is None  # another graph build: re-run
    assert finished(tmp_path, "cugraph", 3, "-1") is None  # never ran
    assert collect(tmp_path, "cugraph", 2, "-1") is None
    write_json(run_file(tmp_path, "cugraph", 2), {"repeat": 2, "edges_fingerprint": "-1"})
    runs = collect(tmp_path, "cugraph", 2, "-1")
    assert [r["repeat"] for r in runs] == [1, 2]
    assert (tmp_path / "runs_cugraph.json").is_file()
