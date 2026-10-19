"""Tests for musin.benchmark.scaling (F5.1): grid, scratch root, summary maths."""

from pathlib import Path

import pytest

from musin.benchmark.scaling import (
    TOTAL,
    Cell,
    bench_config,
    grid,
    prepare_root,
    run_rows,
    summary_rows,
)
from musin.common import spark as spark_module
from musin.common.config import ConfigError, load_config
from musin.common.layers import layer_path
from musin.graph.sample import FULL_ARTIST_TABLES

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "musin-data"
    monkeypatch.setenv("MUSIN_DATA", str(root))
    return root


def test_grid_is_repetition_major():
    cells = grid([1.0, 0.1], [2, 1], 2)
    assert [c.name for c in cells] == [
        "f0.1_c1_r1", "f0.1_c2_r1", "f1_c1_r1", "f1_c2_r1",
        "f0.1_c1_r2", "f0.1_c2_r2", "f1_c1_r2", "f1_c2_r2",
    ]  # fmt: skip


def test_full_fraction_runs_in_full_mode():
    assert Cell(1.0, 4, 1).sample is None
    assert Cell(0.25, 4, 1).sample == 0.25


@pytest.mark.parametrize(
    ("fractions", "cores", "repeats", "message"),
    [
        ([0.0], [1], 1, "fraction"),
        ([1.5], [1], 1, "fraction"),
        ([0.1, 0.1], [1], 1, "duplicate"),
        ([0.1], [0], 1, "cores"),
        ([0.1], [1.5], 1, "cores"),
        ([0.1], [1], 0, "repeats"),
        ([], [1], 1, "at least one"),
    ],
)
def test_invalid_grid_fails_loudly(fractions, cores, repeats, message):
    with pytest.raises(ConfigError, match=message):
        grid(fractions, cores, repeats)


def test_committed_grid_is_fr21(data_root):
    cfg = load_config(REPO_CONFIG)
    cells = grid(
        cfg.get("benchmark.scaling.fractions"),
        cfg.get("benchmark.scaling.cores"),
        cfg.get("benchmark.scaling.repeats"),
    )
    assert {c.fraction for c in cells} == {0.1, 0.25, 0.5, 1.0}
    assert {c.cores for c in cells} == {1, 2, 4, 8}
    assert len(cells) == 16 * cfg.get("benchmark.scaling.repeats")


def test_scratch_root_links_inputs_and_isolates_outputs(data_root):
    cfg = load_config(REPO_CONFIG)
    (cfg.data_path("raw") / "recording").mkdir(parents=True)
    for table in FULL_ARTIST_TABLES:
        (cfg.data_path("clean") / table).mkdir(parents=True)
    root = cfg.data_path("benchmark") / "scaling" / "data"
    prepare_root(cfg, root)
    prepare_root(cfg, root)  # idempotent: links are replaced, not nested

    bcfg = bench_config(cfg, root)
    assert (bcfg.data_path("raw") / "recording").is_dir()
    assert bcfg.data_path("raw").is_symlink()
    for table in FULL_ARTIST_TABLES:
        assert (bcfg.data_path("clean") / table).is_symlink()
    # outputs of both modes land in the scratch root, never in the real layers
    for sample in (None, 0.1):
        for layer in ("clean", "graph"):
            out = layer_path(bcfg, layer, sample)
            assert out.is_relative_to(root)
            assert not out.is_relative_to(cfg.data_path(layer))
    repo_root = bcfg.source.resolve().parent.parent
    assert (repo_root / bcfg.get("reports_dir")).is_relative_to(root)
    assert cfg.get("reports_dir") == "reports/"  # the main config is untouched


def test_scratch_root_refuses_to_replace_a_real_directory(data_root):
    cfg = load_config(REPO_CONFIG)
    cfg.data_path("raw").mkdir(parents=True)
    root = data_root / "bench"
    (root / cfg.get("paths.raw")).mkdir(parents=True)
    with pytest.raises(ConfigError, match="not a symlink"):
        prepare_root(cfg, root)


def test_missing_input_fails(data_root):
    cfg = load_config(REPO_CONFIG)
    with pytest.raises(ConfigError, match="benchmark input missing"):
        prepare_root(cfg, data_root / "bench")


def test_benchmark_refuses_a_variant(data_root):
    cfg = load_config(REPO_CONFIG, "characters")
    with pytest.raises(ConfigError, match="not a variant"):
        bench_config(cfg, data_root / "bench")


def _result(fraction, cores, repeat, total, stage_share=0.25):
    stages = ("clean.credits", "clean.contributions", "clean.years", "graph.build")
    seconds = {s: round(total * stage_share, 2) for s in stages}
    seconds[TOTAL] = total
    return {
        "fraction": fraction,
        "cores": cores,
        "repeat": repeat,
        "started_at_utc": "2026-10-19T00:00:00Z",
        "seconds": seconds,
    }


def test_summary_medians_speedup_efficiency():
    results = [
        _result(1.0, 1, 1, 100.0), _result(1.0, 1, 2, 120.0), _result(1.0, 1, 3, 400.0),
        _result(1.0, 4, 1, 40.0), _result(1.0, 4, 2, 30.0), _result(1.0, 4, 3, 50.0),
        _result(0.5, 1, 1, 60.0), _result(0.5, 4, 1, 20.0),
    ]  # fmt: skip
    rows = {(r["fraction"], r["cores"]): r for r in summary_rows(results)}
    full1, full4 = rows[(1.0, 1)], rows[(1.0, 4)]
    assert (full1["median_s"], full1["min_s"], full1["max_s"]) == (120.0, 100.0, 400.0)
    assert full1["speedup"] == 1.0 and full1["efficiency"] == 1.0
    assert full4["median_s"] == 40.0
    assert full4["speedup"] == 3.0
    assert full4["efficiency"] == 0.75
    assert rows[(0.5, 4)]["vs_full"] == 0.5  # 20 s vs 40 s at the same core count
    assert rows[(0.5, 1)]["speedup"] == 1.0
    assert full4["credits_median_s"] == 10.0
    assert full1["repeats"] == 3 and rows[(0.5, 1)]["repeats"] == 1


def test_run_rows_has_one_row_per_stage_and_total():
    rows = run_rows([_result(0.1, 2, 1, 8.0)])
    assert [r["stage"] for r in rows] == [
        "clean.credits", "clean.contributions", "clean.years", "graph.build", TOTAL,
    ]  # fmt: skip
    assert {r["cores"] for r in rows} == {2}


def test_benchmark_settings_pin_cores_and_event_log(tmp_path, monkeypatch):
    monkeypatch.setattr(spark_module, "_master", "local[*]")
    monkeypatch.setattr(spark_module, "_extra_conf", {})
    spark_module.use_benchmark_settings(4, tmp_path / "events")
    assert spark_module._master == "local[4]"
    assert spark_module._extra_conf["spark.eventLog.enabled"] == "true"
    assert spark_module._extra_conf["spark.eventLog.dir"].startswith("file://")
    assert (tmp_path / "events").is_dir()
    with pytest.raises(ValueError):
        spark_module.use_benchmark_settings(0, tmp_path / "events")
