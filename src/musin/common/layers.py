"""Where a stage reads and writes: full, sample (FR-12, D-019), and variant graphs (D-035)."""

from __future__ import annotations

import re
from pathlib import Path

from musin.common.config import Config, ConfigError

SAMPLE_ROOT = "sample"
VARIANT_ROOT = "variants"
# full | sample-f<fraction>, optionally followed by -<variant> (D-035).
_GRAPH_NAME = re.compile(r"^(?:full|sample-(f[0-9.]+))(?:-([a-z][a-z0-9_]*))?$")


def sample_tag(fraction: float) -> str:
    """Folder name for a sample fraction, e.g. 0.05 -> 'f0.05'."""
    if not 0 < fraction < 1:
        raise ValueError(f"sample fraction must be in (0, 1), got {fraction}")
    return f"f{fraction:g}"


def layer_path(cfg: Config, layer: str, sample: float | None) -> Path:
    """Root of a data layer for recording-level data.

    Full mode: $MUSIN_DATA/<paths.layer>. Sample mode:
    $MUSIN_DATA/sample/f<fraction>/<paths.layer>, so a sample run never
    overwrites full-mode data. Artist-level tables (clean/artists,
    clean/special_artists) are not sampled and are always read from the full
    layer with cfg.data_path().

    A variant config (cfg.variant, D-035) moves only the graph layer, to
    <base>/variants/<variant>/<paths.graph>: a variant changes F3.1 filters
    only, so it reads the same clean layer as the main graph.
    """
    base = cfg.data_root if sample is None else cfg.data_root / SAMPLE_ROOT / sample_tag(sample)
    variant = getattr(cfg, "variant", None)
    if layer == "graph" and variant is not None:
        base = base / VARIANT_ROOT / variant
    return base / cfg.get(f"paths.{layer}")


def graph_name(sample: float | None, variant: str | None = None) -> str:
    """Name of a graph build, its export folder, and the loaded-graph marker (D-031, D-035).

    'full' or 'sample-f<fraction>', plus '-<variant>' for a variant graph.
    """
    base = "full" if sample is None else f"sample-{sample_tag(sample)}"
    return base if variant is None else f"{base}-{variant}"


def parse_graph_name(name: str) -> tuple[str | None, str | None]:
    """(sample tag or None, variant or None) of a graph name; ConfigError if unknown."""
    m = _GRAPH_NAME.match(name)
    if not m:
        raise ConfigError(f"unknown graph name {name!r}")
    return m.group(1), m.group(2)


def results_dir(cfg: Config, graph: str) -> Path:
    """Where the result tables of graph `graph` go.

    full                -> <repo>/<reports_dir>/tables (report-bound, committed)
    full-<v>            -> <repo>/<reports_dir>/tables/variants/<v> (D-035)
    sample-f<x>         -> $MUSIN_DATA/sample/f<x>/reports/tables, next to the
                           sample's other layers (D-019), so development runs
                           never touch the committed reports
    sample-f<x>-<v>     -> $MUSIN_DATA/sample/f<x>/variants/<v>/reports/tables
    """
    tag, variant = parse_graph_name(graph)
    if tag is None:
        repo_root = cfg.source.resolve().parent.parent
        out = repo_root / cfg.get("reports_dir") / "tables"
        return out if variant is None else out / VARIANT_ROOT / variant
    base = cfg.data_root / SAMPLE_ROOT / tag
    if variant is not None:
        base = base / VARIANT_ROOT / variant
    return base / "reports" / "tables"
