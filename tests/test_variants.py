"""Tests for variant graphs (D-035): config overrides, graph names, and paths."""

from pathlib import Path

import pytest
import yaml

from musin.common.config import ConfigError, load_config
from musin.common.layers import graph_name, layer_path, parse_graph_name, results_dir

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "musin-data"
    monkeypatch.setenv("MUSIN_DATA", str(root))
    return root


def config_with_variants(tmp_path: Path, variants: dict) -> Path:
    raw = yaml.safe_load(REPO_CONFIG.read_text(encoding="utf-8"))
    raw["variants"] = variants
    path = tmp_path / "config" / "pipeline.yaml"
    path.parent.mkdir()
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


def test_committed_characters_variant_keeps_character_artists(data_root):
    main = load_config(REPO_CONFIG)
    cfg = load_config(REPO_CONFIG, "characters")
    assert main.get("filters.exclude_artist_types") == ["Character"]  # main graph untouched
    assert cfg.get("filters.exclude_artist_types") == []
    assert cfg.variant == "characters"
    assert cfg.overrides == {"filters.exclude_artist_types": []}
    assert main.variant is None and main.overrides == {}
    # every other setting is the main graph's
    for key in ("filters.max_credit_size", "filters.min_recordings", "analysis.random_seed"):
        assert cfg.get(key) == main.get(key)


def test_unknown_variant_fails(data_root):
    with pytest.raises(ConfigError, match="'nope' not defined"):
        load_config(REPO_CONFIG, "nope")


@pytest.mark.parametrize(
    ("variants", "message"),
    [
        ({"x": {"analysis": {"random_seed": 1}}}, "may only override"),
        ({"x": {"filters": {"special_artists_file": "a.csv"}}}, "may only override"),
        ({"x": {"filters": {"include_classical": "no"}}}, "is str"),
        ({"x": {"filters": {"exclude_artist_types": ["Character"]}}}, "changes nothing"),
        ({"x": {}}, "non-empty mapping"),
        ({"Bad-Name": {"filters": {"min_recordings": 2}}}, "must match"),
    ],
)
def test_invalid_variants_fail_loudly(tmp_path, data_root, variants, message):
    path = config_with_variants(tmp_path, variants)
    load_config(path)  # the main config stays loadable
    with pytest.raises(ConfigError, match=message):
        load_config(path, next(iter(variants)))


def test_several_overrides_apply_together(tmp_path, data_root):
    path = config_with_variants(
        tmp_path,
        {"strict": {"filters": {"include_classical": False, "min_recordings": 3}}},
    )
    cfg = load_config(path, "strict")
    assert cfg.get("filters.include_classical") is False
    assert cfg.get("filters.min_recordings") == 3
    assert cfg.get("filters.exclude_artist_types") == ["Character"]


def test_only_the_graph_layer_moves(data_root):
    main = load_config(REPO_CONFIG)
    cfg = load_config(REPO_CONFIG, "characters")
    assert layer_path(main, "graph", None) == data_root / "graph"
    assert layer_path(cfg, "graph", None) == data_root / "variants" / "characters" / "graph"
    assert layer_path(cfg, "graph", 0.05) == (
        data_root / "sample" / "f0.05" / "variants" / "characters" / "graph"
    )
    # a variant reads the main graph's clean inputs (D-021, D-026)
    assert layer_path(cfg, "clean", None) == layer_path(main, "clean", None)
    assert layer_path(cfg, "clean", 0.05) == data_root / "sample" / "f0.05" / "clean"


def test_graph_names_round_trip():
    cases = {
        (None, None): "full",
        (0.05, None): "sample-f0.05",
        (None, "characters"): "full-characters",
        (0.05, "characters"): "sample-f0.05-characters",
    }
    for (sample, variant), name in cases.items():
        assert graph_name(sample, variant) == name
        assert parse_graph_name(name) == (None if sample is None else "f0.05", variant)
    for bad in ("toy", "full-", "sample-0.05", "full-Characters", "sample-f0.05-a-b"):
        with pytest.raises(ConfigError):
            parse_graph_name(bad)


def test_results_dir_keeps_variants_apart(tmp_path, data_root):
    path = config_with_variants(tmp_path, {"characters": {"filters": {"exclude_artist_types": []}}})
    cfg = load_config(path)
    tables = (tmp_path / "reports" / "tables").resolve()
    assert results_dir(cfg, "full") == tables
    assert results_dir(cfg, "full-characters") == tables / "variants" / "characters"
    assert results_dir(cfg, "sample-f0.05") == data_root / "sample" / "f0.05" / "reports" / "tables"
    assert results_dir(cfg, "sample-f0.05-characters") == (
        data_root / "sample" / "f0.05" / "variants" / "characters" / "reports" / "tables"
    )
