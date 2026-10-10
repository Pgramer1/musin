"""Tests for musin.common.config: the committed config is complete, and
missing keys or environment variables fail loudly."""

from pathlib import Path

import pytest
import yaml

from musin.common.config import REQUIRED_KEYS, ConfigError, load_config

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "musin-data"
    monkeypatch.setenv("MUSIN_DATA", str(root))
    return root


def write_config(tmp_path: Path, raw: dict) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


def committed_raw() -> dict:
    return yaml.safe_load(REPO_CONFIG.read_text(encoding="utf-8"))


def test_committed_config_has_every_required_key(data_root):
    cfg = load_config(REPO_CONFIG)
    for key in REQUIRED_KEYS:
        cfg.get(key)  # raises if absent


def test_committed_config_matches_prd_values(data_root):
    cfg = load_config(REPO_CONFIG)
    assert cfg.get("filters.max_credit_size") == 10
    assert cfg.get("analysis.pagerank.damping") == 0.85
    assert cfg.get("contributions.performed_for") == ["performer", "instrument", "vocal"]
    assert cfg.get("credits.featuring_pattern") == r"(?i)\b(feat|ft|featuring)\b"


def test_data_path_resolves_under_musin_data(data_root):
    cfg = load_config(REPO_CONFIG)
    assert cfg.data_path("raw") == data_root / "raw"


def test_missing_key_fails_loudly(tmp_path, data_root):
    raw = committed_raw()
    del raw["filters"]["max_credit_size"]
    with pytest.raises(ConfigError, match="filters.max_credit_size"):
        load_config(write_config(tmp_path, raw))


def test_null_value_fails_unless_nullable(tmp_path, data_root):
    raw = committed_raw()
    raw["analysis"]["random_seed"] = None
    with pytest.raises(ConfigError, match="analysis.random_seed"):
        load_config(write_config(tmp_path, raw))


def test_snapshot_date_may_be_null(tmp_path, data_root):
    raw = committed_raw()
    raw["snapshot_date"] = None
    assert load_config(write_config(tmp_path, raw)).get("snapshot_date") is None


def test_missing_musin_data_fails(monkeypatch):
    monkeypatch.delenv("MUSIN_DATA", raising=False)
    with pytest.raises(ConfigError, match="MUSIN_DATA"):
        load_config(REPO_CONFIG)


def test_missing_file_fails(tmp_path, data_root):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")
