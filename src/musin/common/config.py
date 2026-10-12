"""Load config/pipeline.yaml and .env-provided settings, failing loudly.

Contract: PRD §8.4 (pipeline.yaml) and §8.5 (.env). There are no defaults in
code: a missing key raises ConfigError naming the key (RULES.md §5).
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any

import yaml

# Every key in PRD §8.4, as dotted paths. A key is added here in the same PR
# as the feature that introduces it (PRD §8.4 "New keys").
REQUIRED_KEYS: tuple[str, ...] = (
    "snapshot_date",
    "source.base_url",
    "source.files",
    "source.signing_key_file",
    "source.signing_key_fingerprint",
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
    "filters.exclude_artist_types",
    "credits.featuring_pattern",
    "contributions.produced_for",
    "contributions.performed_for",
    "genres.mapping_file",
    "years.min_release_year",
    "analysis.min_labelled_neighbours",
    "analysis.top_k_bridges",
    "analysis.top_k_overlap",
    "analysis.top_k_centrality",
    "analysis.pagerank.damping",
    "analysis.pagerank.max_iterations",
    "analysis.betweenness_sampling_size",
    "analysis.betweenness_concurrency",
    "analysis.louvain_runs",
    "analysis.louvain_concurrency",
    "analysis.top_k_communities",
    "analysis.community_example_artists",
    "analysis.separation_max_paths",
    "analysis.random_seed",
    "analysis.concurrency",
    "analysis.decades.from",
    "analysis.decades.to",
    "profiling.min_recordings_grid",
    "profiling.credit_size_cap",
    "profiling.top_k_hubs",
    "profiling.top_k_genres",
    "profiling.top_k_join_phrases",
    "checks.max_row_change",
    "validation.artists_file",
    "variants",
)

# Keys whose value may legitimately be null (e.g. before F1.2 pins the snapshot).
NULLABLE_KEYS: frozenset[str] = frozenset({"snapshot_date"})

# Keys a variant graph may override (D-035): only filters that F3.1 applies, so a
# variant reads the same clean/ layer as the main graph and differs from F3.1 on.
VARIANT_KEYS: frozenset[str] = frozenset(
    {
        "filters.exclude_artist_types",
        "filters.include_classical",
        "filters.min_recordings",
        "filters.max_credit_size",
    }
)
# A variant name is part of folder and graph names ('sample-f0.05-<name>').
VARIANT_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigError(Exception):
    """A required config key or environment variable is missing or invalid."""


class Config:
    """Validated view of pipeline.yaml plus the data root from the environment.

    `variant` names the variant graph (D-035) whose overrides are already
    applied to `raw`, or is None for the main graph; `overrides` lists them.
    """

    def __init__(
        self,
        raw: dict[str, Any],
        source: Path,
        data_root: Path,
        variant: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        self.raw = raw
        self.source = source
        self.data_root = data_root
        self.variant = variant
        self.overrides = overrides or {}

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


def _flatten(node: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """{'a': {'b': 1}} -> {'a.b': 1}."""
    flat: dict[str, Any] = {}
    for key, value in node.items():
        if isinstance(value, dict):
            flat.update(_flatten(value, f"{prefix}{key}."))
        else:
            flat[f"{prefix}{key}"] = value
    return flat


def variant_overrides(cfg: Config, name: str) -> dict[str, Any]:
    """The dotted-key overrides of variant `name` (`variants.<name>`), validated.

    ConfigError if the variant is undefined, badly named, sets a key outside
    VARIANT_KEYS, changes a value's type, or changes nothing.
    """
    variants = cfg.get("variants")
    if not isinstance(variants, dict) or name not in variants:
        known = sorted(variants) if isinstance(variants, dict) else []
        raise ConfigError(f"variant {name!r} not defined under 'variants' in {cfg.source}: {known}")
    if not VARIANT_NAME.match(name):
        raise ConfigError(f"variant name {name!r} must match {VARIANT_NAME.pattern}")
    overlay = variants[name]
    if not isinstance(overlay, dict) or not overlay:
        raise ConfigError(f"variants.{name} must be a non-empty mapping of overrides")
    flat = _flatten(overlay)
    outside = sorted(set(flat) - VARIANT_KEYS)
    if outside:
        raise ConfigError(
            f"variants.{name} sets {outside}; a variant may only override {sorted(VARIANT_KEYS)}"
        )
    for key, value in flat.items():
        if type(value) is not type(cfg.get(key)):
            raise ConfigError(
                f"variants.{name}: {key} is {type(value).__name__}, "
                f"the main value is {type(cfg.get(key)).__name__}"
            )
    if all(cfg.get(key) == value for key, value in flat.items()):
        raise ConfigError(f"variants.{name} changes nothing: it would rebuild the main graph")
    return flat


def load_config(path: str | Path, variant: str | None = None) -> Config:
    """Read and validate pipeline.yaml; resolve the data root from $MUSIN_DATA.

    With `variant`, the overrides under `variants.<variant>` are applied on top
    (D-035): the returned config describes that variant graph.
    """
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
    if variant is None:
        return cfg

    overrides = variant_overrides(cfg, variant)
    merged = copy.deepcopy(raw)
    for key, value in overrides.items():
        *parents, leaf = key.split(".")
        node = merged
        for part in parents:
            node = node[part]
        node[leaf] = value
    return Config(merged, source, cfg.data_root, variant, overrides)
