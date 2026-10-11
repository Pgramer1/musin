"""Shared plumbing for the P4 analyses: which graph Neo4j holds, where results go.

Community Edition holds one graph at a time (RULES §7), so a P4 stage has no
--sample flag: it analyses whatever $MUSIN_DATA/neo4j/loaded_graph.json says
is loaded (F3.4, D-031) and names that graph and its edges fingerprint in
every output.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import pandas as pd

from musin.common.config import Config, ConfigError
from musin.common.layers import SAMPLE_ROOT
from musin.export.neo4j_check import LOADED_MARKER

CONNECT_WAIT_S = 180
_SAMPLE_GRAPH = re.compile(r"^sample-(f[0-9.]+)$")


def loaded_graph(cfg: Config) -> dict:
    """The verified loaded-graph marker; ConfigError if missing or the F1.5 toy graph."""
    marker = cfg.data_root / "neo4j" / LOADED_MARKER
    if not marker.is_file():
        raise ConfigError(f"{marker} missing: Neo4j content unknown; run neo4j/import_graph.sh")
    loaded = json.loads(marker.read_text())
    if loaded["graph"] == "toy":
        raise ConfigError("Neo4j holds the F1.5 toy graph; import a real graph first")
    return loaded


def results_dir(cfg: Config, graph: str) -> Path:
    """Where the result tables of `graph` go.

    full -> <repo>/<reports_dir>/tables (report-bound, committed).
    sample-f<x> -> $MUSIN_DATA/sample/f<x>/reports/tables, next to the sample's
    other layers (D-019), so development runs never touch the committed reports.
    """
    if graph == "full":
        repo_root = cfg.source.resolve().parent.parent
        return repo_root / cfg.get("reports_dir") / "tables"
    m = _SAMPLE_GRAPH.match(graph)
    if not m:
        raise ConfigError(f"unknown loaded graph {graph!r}")
    return cfg.data_root / SAMPLE_ROOT / m.group(1) / "reports" / "tables"


def write_table(frame: pd.DataFrame, path: Path) -> Path:
    """Write a result table as CSV with LF line ends (stable diffs in git)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    return path
