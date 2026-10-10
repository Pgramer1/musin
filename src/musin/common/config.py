"""Load config/pipeline.yaml and .env-provided settings, failing loudly.

Contract: PRD §8.4 (pipeline.yaml) and §8.5 (.env). There are no defaults in
code: a missing key raises ConfigError naming the key (RULES.md §5).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Every key in PRD §8.4, as dotted paths. A key is added here in the same PR
# as the feature that introduces it (PRD §8.4 "New keys").
REQUIRED_KEYS: tuple[str, ...] = (
    "snapshot_date",
    "paths.dumps",
    "paths.raw",
    "paths.clean",
    "paths.graph",
    "paths.import",
    "reports_dir",
    "filters.special_artists_file",
    "filters.min_recordings",
    "filters.max_credit_size",
    "filters.include_classical",
    "filters.use_release_level_credits",
    "credits.featuring_pattern",
    "contributions.produced_for",
    "contributions.performed_for",
    "genres.mapping_file",
    "analysis.min_labelled_neighbours",
    "analysis.top_k_bridges",
    "analysis.top_k_overlap",
    "analysis.pagerank.damping",
    "analysis.pagerank.max_iterations",
    "analysis.betweenness_sampling_size",
    "analysis.louvain_runs",
    "analysis.random_seed",
    "analysis.decades.from",
    "analysis.decades.to",
)

# Keys whose value may legitimately be null (e.g. before F1.2 pins the snapshot).
NULLABLE_KEYS: frozenset[str] = frozenset({"snapshot_date"})


class ConfigError(Exception):
    """A required config key or environment variable is missing or invalid."""


class Config:
    """Validated view of pipeline.yaml plus the data root from the environment."""

    def __init__(self, raw: dict[str, Any], source: Path, data_root: Path) -> None:
        self.raw = raw
        self.source = source
        self.data_root = data_root

    def get(self, dotted_key: str) -> Any:
        """Return the value at a dotted key; raise ConfigError if absent."""
        node: Any = self.raw
        for part in dotted_key.split("."):
            if not isinstance(node, dict) or part not in node:
                raise ConfigError(f"Missing config key '{dotted_key}' in {self.source}")
            node = node[part]
        return node

    def data_path(self, layer: str) -> Path:
        """Absolute path of a data layer (`paths.<layer>`) under $MUSIN_DATA."""
        return self.data_root / self.get(f"paths.{layer}")


def require_env(name: str) -> str:
    """Return a non-empty environment variable or raise ConfigError."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Environment variable {name} is not set (see .env.example)")
    return value


def load_config(path: str | Path) -> Config:
    """Read and validate pipeline.yaml; resolve the data root from $MUSIN_DATA."""
    source = Path(path)
    if not source.is_file():
        raise ConfigError(f"Config file not found: {source}")
    with source.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ConfigError(f"{source} does not contain a YAML mapping")

    cfg = Config(raw, source, Path(require_env("MUSIN_DATA")))
    for key in REQUIRED_KEYS:
        if cfg.get(key) is None and key not in NULLABLE_KEYS:
            raise ConfigError(f"Config key '{key}' in {source} is null")
    return cfg
