"""Read the report tables and check they describe one graph build (F6.1).

Every P4/P5 table stamps its graph name and edges fingerprint on each row.
The report combines many of them, so before anything is drawn the stage
checks that all tables of the main graph (reports/tables) name `full` and one
fingerprint, and that each variant folder names `full-<variant>` and one
fingerprint of its own. A table left over from another build would otherwise
mix two graphs in one report without anyone noticing.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

MAIN_GRAPH = "full"


class StaleTableError(RuntimeError):
    """Report tables disagree about which graph build they describe."""


class Tables:
    """Lazy, cached reader of the CSVs in one results folder."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._cache: dict[str, pd.DataFrame] = {}

    def __call__(self, name: str) -> pd.DataFrame:
        if name not in self._cache:
            path = self.root / name
            if not path.is_file():
                raise FileNotFoundError(f"{path} missing; run the stage that writes it first")
            # Fingerprints are long integers; keep them (and MBIDs) as text.
            self._cache[name] = pd.read_csv(
                path, dtype={"edges_fingerprint": str}, keep_default_na=True
            )
        return self._cache[name]

    def csv_names(self) -> list[str]:
        """Every CSV directly in this folder, sorted."""
        return sorted(p.name for p in self.root.glob("*.csv"))


def build_stamps(tables: Tables) -> dict[str, set[tuple[str, str | None]]]:
    """{csv name: {(graph, edges_fingerprint or None)}} for every stamped CSV."""
    stamps: dict[str, set[tuple[str, str | None]]] = {}
    for name in tables.csv_names():
        frame = tables(name)
        if "graph" not in frame.columns or frame.empty:
            continue
        fps = (
            frame["edges_fingerprint"]
            if "edges_fingerprint" in frame.columns
            else pd.Series([None] * len(frame))
        )
        stamps[name] = set(zip(frame["graph"].astype(str), fps, strict=True))
    return stamps


def check_one_build(tables: Tables, graph: str) -> str:
    """The single edges fingerprint of the tables in `tables`; StaleTableError otherwise.

    Every stamped table must name `graph`; tables that carry a fingerprint must
    all carry the same one. Tables stamped with the graph name only (F4.4's
    trends) are checked for the name.
    """
    stamps = build_stamps(tables)
    if not stamps:
        raise StaleTableError(f"no stamped tables in {tables.root}")
    wrong_graph = {n: s for n, s in stamps.items() if {g for g, _ in s} != {graph}}
    fingerprints = {fp for s in stamps.values() for _, fp in s if fp is not None}
    if wrong_graph or len(fingerprints) != 1:
        detail = "; ".join(f"{n}: {sorted(map(str, s))}" for n, s in sorted(stamps.items()))
        raise StaleTableError(
            f"tables in {tables.root} do not describe one build of graph {graph!r}: {detail}"
        )
    return fingerprints.pop()
