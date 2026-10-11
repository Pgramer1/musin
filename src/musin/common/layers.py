"""Where a stage reads and writes, in full and sample mode (FR-12, D-019)."""

from __future__ import annotations

from pathlib import Path

from musin.common.config import Config

SAMPLE_ROOT = "sample"


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
    """
    if sample is None:
        return cfg.data_path(layer)
    return cfg.data_root / SAMPLE_ROOT / sample_tag(sample) / cfg.get(f"paths.{layer}")
