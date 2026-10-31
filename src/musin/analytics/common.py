"""Shared plumbing for the P4 analyses: which graph Neo4j holds, where results go.

Community Edition holds one graph at a time (RULES §7), so a P4 stage has no
--sample flag: it analyses whatever $MUSIN_DATA/neo4j/loaded_graph.json says
is loaded (F3.4, D-031) and names that graph and its edges fingerprint in
every output. Results of a variant graph (D-035) go to their own folder
(musin.common.layers.results_dir).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

from musin.common.config import Config, ConfigError
from musin.common.layers import results_dir  # noqa: F401  (re-exported for P4 stages)
from musin.export.neo4j_check import LOADED_MARKER

CONNECT_WAIT_S = 180


def loaded_graph(cfg: Config) -> dict:
    """The verified loaded-graph marker; ConfigError if missing or the F1.5 toy graph."""
    marker = cfg.data_root / "neo4j" / LOADED_MARKER
    if not marker.is_file():
        raise ConfigError(f"{marker} missing: Neo4j content unknown; run neo4j/import_graph.sh")
    loaded = json.loads(marker.read_text())
    if loaded["graph"] == "toy":
        raise ConfigError("Neo4j holds the F1.5 toy graph; import a real graph first")
    return loaded


def check_still_loaded(cfg: Config, loaded: dict) -> None:
    """RuntimeError unless the marker still names the graph a stage started on.

    Neo4j has one database. An import during a P4 run (e.g. from another
    shell) would silently mix two graphs, so each stage re-reads the marker
    after its last Neo4j query and refuses to write results otherwise.
    """
    marker = cfg.data_root / "neo4j" / LOADED_MARKER
    now = json.loads(marker.read_text()) if marker.is_file() else None
    if now is None or now.get("graph_fingerprints") != loaded["graph_fingerprints"]:
        raise RuntimeError(
            f"the loaded graph changed during the run (started on {loaded['graph']}, "
            f"marker now {now['graph'] if now else 'missing'}); results discarded"
        )


def write_table(frame: pd.DataFrame, path: Path) -> Path:
    """Write a result table as CSV with LF line ends (stable diffs in git)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    return path
