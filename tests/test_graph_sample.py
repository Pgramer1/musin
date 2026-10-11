"""Tests for musin.graph.sample (F3.2) on tiny frames."""

from musin.graph.build import CO_CREDITED, FEATURED, PERFORMED_FOR, PRODUCED_FOR
from musin.graph.sample import comparison_rows, fingerprint, graph_profile, repo_commit

EDGES = "src_id long, dst_id long, edge_type string, weight long, first_year int, last_year int"


def test_fingerprint_ignores_order_and_partitioning(spark):
    rows = [(1, "a", None), (2, "b", 3.5), (3, None, 0.0)]
    schema = "id long, s string, x double"
    one = spark.createDataFrame(rows, schema)
    other = spark.createDataFrame(list(reversed(rows)), schema).repartition(3)
    assert fingerprint(one) == fingerprint(other)
    assert fingerprint(one)["rows"] == 3


def test_fingerprint_detects_changed_and_missing_rows(spark):
    schema = "id long, s string"
    base = fingerprint(spark.createDataFrame([(1, "a"), (2, "b")], schema))
    changed = fingerprint(spark.createDataFrame([(1, "a"), (2, "c")], schema))
    fewer = fingerprint(spark.createDataFrame([(1, "a")], schema))
    assert base["fingerprint"] != changed["fingerprint"]
    assert base != fewer


def test_graph_profile(spark):
    nodes = spark.createDataFrame(
        [(1, "rock"), (2, "classical"), (3, None), (4, "rock")],
        "artist_id long, primary_genre string",
    )
    edges = spark.createDataFrame(
        [
            (1, 2, CO_CREDITED, 3, 1995, 2001),
            (1, 2, PERFORMED_FOR, 1, 1995, 1995),  # same pair, other type: one collaborator
            (1, 3, FEATURED, 1, None, None),
            (4, 1, PRODUCED_FOR, 5, 2003, 2004),
        ],
        EDGES,
    )
    by_decade = spark.createDataFrame(
        [(1, 2, CO_CREDITED, 1990, 2), (1, 2, CO_CREDITED, 2000, 1), (4, 1, PRODUCED_FOR, 2000, 5)],
        "src_id long, dst_id long, edge_type string, decade int, weight long",
    )
    m = graph_profile(nodes, edges, by_decade)
    assert (m["nodes"], m["edges"], m["total_weight"]) == (4, 4, 10)
    assert m["weight_share_PRODUCED_FOR"] == 0.5
    assert m["edge_share_FEATURED"] == 0.25
    assert m["nodes_labelled_share"] == 0.75
    assert m["nodes_classical_share"] == 0.25
    assert m["edges_labelled_share"] == 0.75  # the FEATURED edge has an unlabelled end
    assert m["edges_weight_1_share"] == 0.5
    assert m["degree_max"] == 3  # artist 1: partners 2, 3, 4
    assert m["degree_mean"] == 1.5  # 3 pairs, 4 artists
    assert (m["weight_share_1990s"], m["weight_share_2000s"]) == (0.2, 0.6)  # 0.2 has no year


def test_comparison_rows():
    rows = comparison_rows(
        {"nodes": 100, "zero": 0, "weight_share_1950s": 0.1}, {"nodes": 5, "zero": 0}
    )
    by_metric = {r["metric"]: r for r in rows}
    assert by_metric["nodes"]["sample_over_full"] == 0.05
    assert by_metric["zero"]["sample_over_full"] == ""
    assert by_metric["weight_share_1950s"]["sample"] == ""


def test_repo_commit_reads_loose_and_packed_refs(tmp_path):
    git = tmp_path / ".git"
    (git / "refs" / "heads").mkdir(parents=True)
    (git / "HEAD").write_text("ref: refs/heads/main\n")
    (git / "refs" / "heads" / "main").write_text("abc123\n")
    assert repo_commit(tmp_path) == "abc123"
    (git / "refs" / "heads" / "main").unlink()
    (git / "packed-refs").write_text("# pack-refs\ndef456 refs/heads/main\n")
    assert repo_commit(tmp_path) == "def456"
    assert repo_commit(tmp_path / "nowhere") is None
