"""Tests for musin.report (F6.1) on tiny hand-made tables."""

import pandas as pd
import pytest

from musin.report import derived, figures, manifest
from musin.report.formats import (
    ReportTable,
    fmt_int,
    fmt_pct,
    fmt_seconds,
    latex_escape,
    to_latex,
    to_markdown,
    write_table_files,
)
from musin.report.inputs import StaleTableError, Tables, check_one_build

FP = "-123"


# --- formats ---------------------------------------------------------------


def test_number_formats():
    assert fmt_int(1234567.0) == "1,234,567"
    assert fmt_int(float("nan")) == "–"
    assert fmt_pct(0.28406) == "28.4 %"
    assert fmt_pct(0.99993, 2) == "99.99 %"
    assert fmt_pct(None) == "–"
    assert [fmt_seconds(v) for v in (0.2, 3.83, 469.74, 1224.2)] == [
        "0.20 s",
        "3.8 s",
        "470 s",
        "1,224 s",
    ]


def test_latex_escape():
    assert latex_escape("r&b 50% #1 a_b {x} ~^\\") == (
        r"r\&b 50\% \#1 a\_b \{x\} \textasciitilde{}\textasciicircum{}\textbackslash{}"
    )


def _table(**kw):
    base = dict(
        name="t",
        caption="Cap",
        columns=["A", "B"],
        align="lr",
        rows=[["r&b | soul", "5 %"]],
        sources=["x.csv"],
        notes=["Note."],
    )
    return ReportTable(**(base | kw))


def test_report_table_shape_is_checked():
    with pytest.raises(ValueError, match="columns"):
        _table(align="l")
    with pytest.raises(ValueError, match="cells"):
        _table(rows=[["only one"]])


def test_markdown_and_latex(tmp_path):
    md = to_markdown(_table())
    assert "reports/tables/x.csv" in md.splitlines()[0]
    assert "|:---|---:|" in md
    assert r"| r&b \| soul | 5 % |" in md  # pipe escaped, no other change
    assert md.rstrip().endswith("Note.")
    tex = to_latex(_table())
    assert r"\caption{Cap. Note.}" in tex
    assert r"\label{tab:t}" in tex
    assert r"r\&b | soul & 5 \% \\" in tex
    assert {p.name for p in write_table_files(_table(), tmp_path)} == {"t.md", "t.tex"}


# --- inputs ----------------------------------------------------------------


def _write(path, frame):
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def test_check_one_build(tmp_path):
    _write(
        tmp_path / "a.csv", pd.DataFrame({"x": [1], "graph": ["full"], "edges_fingerprint": [FP]})
    )
    _write(tmp_path / "b.csv", pd.DataFrame({"x": [1, 2], "graph": ["full", "full"]}))
    _write(tmp_path / "c.csv", pd.DataFrame({"metric": ["m"], "value": [1]}))  # unstamped
    assert check_one_build(Tables(tmp_path), "full") == FP

    _write(tmp_path / "d.csv", pd.DataFrame({"graph": ["full"], "edges_fingerprint": ["-999"]}))
    with pytest.raises(StaleTableError, match="d.csv"):
        check_one_build(Tables(tmp_path), "full")


def test_check_one_build_wrong_graph(tmp_path):
    _write(tmp_path / "a.csv", pd.DataFrame({"graph": ["sample-f0.05"], "edges_fingerprint": [FP]}))
    with pytest.raises(StaleTableError):
        check_one_build(Tables(tmp_path), "full")
    with pytest.raises(FileNotFoundError):
        Tables(tmp_path)("missing.csv")


# --- derived ---------------------------------------------------------------

MAIN_FILTERS = {"exclude_artist_types": ["Character"], "include_classical": False}


def test_admitted_predicate():
    chars = derived.admitted_predicate(
        MAIN_FILTERS, {"exclude_artist_types": [], "include_classical": False}
    )
    classical = derived.admitted_predicate(
        MAIN_FILTERS, {"exclude_artist_types": ["Character"], "include_classical": True}
    )
    miku = pd.Series({"type": "Character", "primary_genre": "electronic"})
    lso = pd.Series({"type": "Orchestra", "primary_genre": "classical"})
    assert chars(miku) and not chars(lso)
    assert classical(lso) and not classical(miku)


def _top(graph, fp, rows):
    """rows: (projection, measure, rank, mbid, name, type, primary_genre)."""
    cols = ["projection", "measure", "rank", "mbid", "name", "type", "primary_genre"]
    return pd.DataFrame(rows, columns=cols).assign(graph=graph, edges_fingerprint=fp)


def _full_lists(entries):
    """Every (projection, measure) gets the same two-artist list."""
    rows = []
    for p in ("collab_undirected", "credit_only"):
        for m in derived.MEASURES:
            rows += [(p, m, rank, *e) for rank, e in enumerate(entries, start=1)]
    return rows


def test_sensitivity_top_lists():
    main = _top("full", FP, _full_lists([("a", "A", "Person", "rock"), ("b", "B", "Group", "pop")]))
    var = _top(
        "full-characters",
        "-7",
        _full_lists([("m", "Miku", "Character", "electronic"), ("a", "A", "Person", "rock")]),
    )
    bridges_main = pd.DataFrame(
        {
            "bridge_rank": [1, 2],
            "mbid": ["a", "c"],
            "name": ["A", "C"],
            "type": ["Person", "Person"],
            "primary_genre": ["rock", "jazz"],
        }
    )
    bridges_var = bridges_main.assign(mbid=["a", "d"], name=["A", "D"])
    admitted = derived.admitted_predicate(
        MAIN_FILTERS, {"exclude_artist_types": [], "include_classical": False}
    )
    out = derived.sensitivity_top_lists(
        "characters", main, var, bridges_main, bridges_var, 2, 2, admitted
    )
    assert len(out) == 2 * 3 + 1
    first = out.iloc[0]
    assert (first["common"], first["entrants"], first["admitted_entrants"]) == (1, 1, 1)
    assert first["jaccard"] == pytest.approx(1 / 3)
    assert first["admitted_names"] == "Miku #1"
    last = out.iloc[-1]
    assert (last["list"], last["admitted_entrants"], last["other_entrant_names"]) == (
        "bridges",
        0,
        "D #2",
    )
    assert out["graph_variant"].unique().tolist() == ["full-characters"]


def test_sensitivity_top_lists_k_limits_the_list():
    main = _top("full", FP, _full_lists([("a", "A", "Person", "rock"), ("b", "B", "Group", "pop")]))
    var = _top(
        "full-x", "-7", _full_lists([("a", "A", "Person", "rock"), ("c", "C", "Person", "pop")])
    )
    bridges = pd.DataFrame(
        {
            "bridge_rank": [1],
            "mbid": ["a"],
            "name": ["A"],
            "type": ["Person"],
            "primary_genre": ["rock"],
        }
    )
    out = derived.sensitivity_top_lists("x", main, var, bridges, bridges, 1, 1, lambda _r: False)
    assert (out["jaccard"] == 1.0).all() and (out["entrants"] == 0).all()


def _agree(graph, fp, rho, jac):
    return pd.DataFrame(
        {
            "comparison": ["collab_undirected"],
            "measure_a": ["pagerank"],
            "measure_b": ["betweenness"],
            "n_artists": [10],
            "spearman_rho": [rho],
            "top_k": [100],
            "jaccard_top_k": [jac],
            "graph": [graph],
            "edges_fingerprint": [fp],
        }
    )


def test_sensitivity_agreement():
    out = derived.sensitivity_agreement(
        "v", _agree("full", FP, 0.5, 0.2), _agree("full-v", "-7", 0.55, 0.1)
    )
    row = out.iloc[0]
    assert row["rho_delta"] == pytest.approx(0.05)
    assert row["jaccard_delta"] == pytest.approx(-0.1)
    assert (row["graph_main"], row["graph_variant"]) == ("full", "full-v")


def test_sensitivity_agreement_rows_must_match():
    other = _agree("full-v", "-7", 0.5, 0.2).assign(measure_b="degreeWeighted")
    with pytest.raises(ValueError, match="differ"):
        derived.sensitivity_agreement("v", _agree("full", FP, 0.5, 0.2), other)


def _summary():
    rows = {
        "recordings_in": 100,
        "recordings_over_10_artists": 2,
        "pair_instances_skipped_over_10": 45,
        "excluded_artists_by_type": 3,
        "excluded_artists_classical": 4,
        "edges_before_node_rule": 30,
        "edges_removed_by_min_recordings": 1,
        "nodes": 20,
    }
    for t in ("CO_CREDITED", "FEATURED", "PRODUCED_FOR", "PERFORMED_FOR"):
        for i, f in enumerate(derived.FATES):
            rows[f"instances_{t}_{f}"] = 10 if f == "kept" else i
    return pd.DataFrame({"metric": list(rows), "value": list(rows.values())})


def test_instance_fates():
    out = derived.instance_fates(_summary(), "full", FP)
    assert len(out) == 4 * len(derived.FATES)
    co = out[out["edge_type"] == "CO_CREDITED"].set_index("fate")
    assert co.loc["kept", "instances"] == 10
    assert co["share_of_type"].sum() == pytest.approx(1.0)
    assert co.loc["kept", "share_of_type"] == pytest.approx(10 / (10 + sum(range(1, 6))))


def test_instance_fates_missing_metric():
    summary = _summary()
    summary = summary[summary["metric"] != "instances_FEATURED_band_member"]
    with pytest.raises(ValueError, match="FEATURED"):
        derived.instance_fates(summary, "full", FP)


def test_pipeline_funnel():
    row_counts = pd.DataFrame(
        {
            "table": ["recording", "track", "artist", "artist_credit_name", "l_artist_recording"],
            "rows": [1, 2, 3, 4, 5],
        }
    )
    profile = pd.DataFrame(
        {"section": ["base"], "metric": ["credited artists (on >= 1 recording)"], "value": ["6"]}
    )
    projections = pd.DataFrame(
        {
            "projection": ["collab_undirected", "credit_only"],
            "pairs": [25, 20],
            "total_weight": [40, 30],
        }
    )
    out = derived.pipeline_funnel(row_counts, profile, _summary(), projections, "full", FP)
    value = dict(zip(out["quantity"], out["value"], strict=True))
    assert value["recordings"] == 1
    assert value["credited artists (on at least one recording)"] == 6
    assert value["recordings over the credit-size cap (no edges)"] == 2
    assert value["edges (artist pair × type)"] == 29
    assert value["edge instances kept"] == 40
    assert value["edge instances dropped (all reasons)"] == 4 * sum(range(1, 6))
    assert value["COLLAB artist pairs"] == 25
    assert out["step"].tolist() == list(range(1, len(out) + 1))


# --- manifest --------------------------------------------------------------


def test_classify():
    assert manifest.classify("tables/genre_matrix.csv")[0] == "RQ1 — genre bridges"
    assert manifest.classify("tables/genre_coverage.csv")[1] == "F2.5"
    assert manifest.classify("tables/variants/classical/centrality_top.csv")[1] == "F4.7"
    assert manifest.classify("tables/variants/classical/graph_checks.csv")[1] == "F4.7"
    assert manifest.classify("figures/benchmark_pagerank.png")[1] == "F6.1"
    assert manifest.classify("tables/handmade.csv") is None


def test_render_lists_uncatalogued(tmp_path):
    for rel in ("tables/centrality_top.csv", "figures/genre_matrix.png", "tables/stray.csv"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("x")
    text, unlisted = manifest.render(tmp_path, [_table(name="rq4_x", caption="Top list")])
    assert unlisted == ["tables/stray.csv"]
    assert "## RQ4 — centrality" in text and "tables/report/rq4_x.md" in text
    assert "| F4.1 |" in text and "| F6.1 |" in text
    with pytest.raises(ValueError):
        manifest.report_table_section("nosuchprefix")


# --- figures (smoke) -------------------------------------------------------


def test_figures_render(tmp_path):
    summary = pd.DataFrame(
        {
            "engine": ["gds", "graphframes"],
            **{
                f"{p}_{s}_s": [1.0, 10.0]
                for p in ("load", "compute", "output", "total")
                for s in ("median", "min", "max")
            },
            "graph": ["full", "full"],
        }
    )
    agreement = pd.DataFrame(
        {
            "a": ["gds"],
            "b": ["graphframes"],
            "spearman_rho": [0.99],
            "top_k": [100],
            "jaccard_top_k": [1.0],
        }
    )
    assert figures.benchmark_pagerank(summary, agreement, tmp_path / "p.png").stat().st_size > 0

    top = pd.DataFrame(
        {
            "community": [1],
            "size": [100],
            "labelled_share": [0.3],
            "share_folk & world": [0.6],
            "share_r&b & soul": [0.05],
            "share_rock": [0.35],
            "example_artists": ["A; B; C; D"],
            "graph": ["full"],
        }
    )
    assert figures.communities_composition(top, tmp_path / "c.png").is_file()


def test_bucket_label():
    assert figures._bucket_label("1000-9999") == "1,000–9,999"
    assert figures._bucket_label("2-9") == "2–9"
