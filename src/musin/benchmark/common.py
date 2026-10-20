"""Shared, dependency-light helpers for the PageRank engine benchmarks (F5.2, F5.3).

Imports only the standard library and musin.common.config, so the RAPIDS
container (no PySpark, no Neo4j driver) can use it too.
"""

from __future__ import annotations

import json
from pathlib import Path

from musin.common.config import Config, ConfigError

# Same file as musin.export.neo4j_check.LOADED_MARKER (that module needs PySpark).
LOADED_MARKER = "loaded_graph.json"
PHASES = ("load", "compute", "output")
TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


def settings(cfg: Config) -> dict:
    """The algorithm settings every engine shares (D-042)."""
    return {
        "damping": float(cfg.get("analysis.pagerank.damping")),
        "max_iterations": int(cfg.get("analysis.pagerank.max_iterations")),
        "tolerance": float(cfg.get("benchmark.pagerank.tolerance")),
        "cores": int(cfg.get("analysis.concurrency")),
        "repeats": int(cfg.get("benchmark.pagerank.repeats")),
    }


def read_loaded(cfg: Config) -> dict:
    """The loaded-graph marker (as musin.analytics.common.loaded_graph, without its imports)."""
    marker = cfg.data_root / "neo4j" / LOADED_MARKER
    if not marker.is_file():
        raise ConfigError(f"{marker} missing: Neo4j content unknown; run neo4j/import_graph.sh")
    loaded = json.loads(marker.read_text())
    if loaded["graph"] == "toy":
        raise ConfigError("Neo4j holds the F1.5 toy graph; import a real graph first")
    return loaded


def base_dir(cfg: Config, graph: str) -> Path:
    """Per-graph result folder of the PageRank benchmarks."""
    return cfg.data_path("benchmark") / "pagerank" / graph


def write_json(target: Path, data) -> None:
    """Write via a temp file, so an interrupted run leaves no partial result."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(target)


def run_file(base: Path, engine: str, repeat: int) -> Path:
    """Result of one repetition of one engine."""
    return base / f"run_{engine}_r{repeat}.json"


def finished(base: Path, engine: str, repeat: int, fingerprint: str) -> dict | None:
    """The saved result of a repetition, or None if it must (re)run.

    A result from another graph build (different edges fingerprint) does not
    count, so a rebuilt graph is never reported with stale timings.
    """
    path = run_file(base, engine, repeat)
    if not path.is_file():
        return None
    run = json.loads(path.read_text())
    return run if run.get("edges_fingerprint") == fingerprint else None


def collect(base: Path, engine: str, repeats: int, fingerprint: str) -> list[dict] | None:
    """All repetitions of an engine if every one has finished, in order; else None."""
    runs = [finished(base, engine, r, fingerprint) for r in range(1, repeats + 1)]
    if any(r is None for r in runs):
        return None
    write_json(base / f"runs_{engine}.json", runs)
    return runs
