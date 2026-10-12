"""Report figures for F4.3: the genre x genre heatmap and bridge ego networks.

Static PNGs in the project chart style (musin.profiling.figures). Colours
come from the validated reference palette: the sequential blue ramp for the
heatmap; for ego networks, whose nodes sit on a circle like a scatter, only
the first three categorical slots (blue, orange, aqua), which are the ones that
pass the all-pairs colour checks. Every further genre folds into "other
genres", and genre groups are also labelled on the figure, so identity never
rests on colour alone.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from musin.profiling.figures import DPI, SURFACE, TEXT, TEXT_2, _style, plt

CATEGORICAL = ("#2a78d6", "#eb6834", "#1baf7a")  # palette slots 1-3 (all-pairs safe)
OTHER = "#9a9893"
UNLABELLED = "#d3d2cd"
BLUE_RAMP = ("#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b")
NAMED_NEIGHBOURS = 8
# Artist names are in many scripts; DejaVu has no CJK glyphs, so fall back to
# Noto Sans CJK (fonts-noto-cjk in docker/spark/Dockerfile).
FONT_FAMILY = ["DejaVu Sans", "Noto Sans CJK JP"]


def _style_figure() -> None:
    """Project chart style plus the CJK font fallback."""
    _style()
    plt.rcParams["font.family"] = FONT_FAMILY


def ego_layout(nb: pd.DataFrame, max_nb: int) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """Choose, group and place the neighbours of one ego network.

    In: artistId, w, name, primaryGenre for every neighbour. Out: the
    `max_nb` strongest neighbours (ties by artistId) with group, colour and
    angle, ordered around the circle by group (the three genres with most
    weight among them, then other genres, then unlabelled) and by weight;
    plus the (group, colour) legend in that order.
    """
    shown = nb.sort_values(["w", "artistId"], ascending=[False, True], kind="mergesort").head(
        max_nb
    )
    shown = shown.copy()
    genre_w = (
        shown.dropna(subset=["primaryGenre"])
        .groupby("primaryGenre")["w"]
        .sum()
        .sort_values(ascending=False, kind="mergesort")
    )
    top = list(genre_w.index[:3])
    colour = dict(zip(top, CATEGORICAL, strict=False))
    shown["group"] = shown["primaryGenre"].map(
        lambda g: g if g in colour else ("unlabelled" if pd.isna(g) else "other genres")
    )
    order = {g: i for i, g in enumerate([*top, "other genres", "unlabelled"])}
    shown["order"] = shown["group"].map(order)
    shown = shown.sort_values(
        ["order", "w", "artistId"], ascending=[True, False, True], kind="mergesort"
    ).reset_index(drop=True)
    shown["colour"] = shown["group"].map(
        lambda g: colour.get(g, UNLABELLED if g == "unlabelled" else OTHER)
    )
    shown["angle"] = np.linspace(0, 2 * math.pi, len(shown), endpoint=False) + math.pi / 2
    legend = [(g, colour[g]) for g in top]
    if (shown["group"] == "other genres").any():
        legend.append(("other genres", OTHER))
    if (shown["group"] == "unlabelled").any():
        legend.append(("unlabelled", UNLABELLED))
    return shown, legend


def plot_ego(
    name: str, genre: str | None, nb: pd.DataFrame, max_nb: int, target: Path, graph: str
) -> Path:
    """Radial ego network: the bridge in the centre, neighbours on a circle by genre."""
    _style_figure()
    shown, legend = ego_layout(nb, max_nb)
    fig, ax = plt.subplots(figsize=(7.2, 7.2))
    ax.set_aspect("equal")
    ax.axis("off")
    wmax = float(shown["w"].max())
    x, y = np.cos(shown["angle"]), np.sin(shown["angle"])
    for xi, yi, w, c in zip(x, y, shown["w"], shown["colour"], strict=True):
        ax.plot(
            [0, xi],
            [0, yi],
            color=c,
            alpha=0.55,
            linewidth=0.4 + 2.6 * math.log1p(w) / math.log1p(wmax),
            zorder=1,
        )
    ax.scatter(x, y, s=34, c=shown["colour"], edgecolors=SURFACE, linewidths=1.5, zorder=2)
    ax.scatter([0], [0], s=160, c=TEXT, edgecolors=SURFACE, linewidths=2, zorder=3)
    ax.text(
        0, -0.09, name, ha="center", va="top", fontsize=10, fontweight="bold", color=TEXT, zorder=4
    )
    # Group sizes go in the legend, not on the circle, where they collided with
    # the neighbour names.
    sizes = shown["group"].value_counts()
    named = shown.sort_values(["w", "artistId"], ascending=[False, True]).head(NAMED_NEIGHBOURS)
    for r in named.itertuples():
        ca, sa = math.cos(r.angle), math.sin(r.angle)
        ax.text(
            1.08 * ca,
            1.08 * sa,
            f"{r.name} ({r.w})",
            fontsize=6.5,
            color=TEXT,
            ha="left" if ca >= 0 else "right",
            va="center",
            rotation=math.degrees(r.angle) if ca >= 0 else math.degrees(r.angle) + 180,
            rotation_mode="anchor",
        )
    ax.set_xlim(-1.6, 1.6)
    ax.set_ylim(-1.6, 1.6)
    handles = [
        plt.Line2D([], [], marker="o", linestyle="", color=c, label=f"{g} · {sizes[g]}")
        for g, c in legend
    ]
    ax.legend(
        handles=handles,
        loc="lower left",
        fontsize=8,
        title="neighbour primary genre",
        title_fontsize=8,
    )
    ax.set_title(
        f"Collaborators of {name} ({genre or 'no primary genre'}): "
        f"{len(shown)} strongest of {len(nb)}",
        loc="left",
    )
    fig.text(
        0.01, 0.01, f"line width: shared recordings (log) · graph {graph}", fontsize=7, color=TEXT_2
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(target, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    return target


def plot_matrix(matrix: pd.DataFrame, genres: list[str], target: Path, graph: str) -> Path:
    """Heatmap of the genre x genre matrix, each row normalised to its genre's total.

    Rows and columns are ordered by total labelled weight. Cell (a, b) is the
    share of genre a's labelled collaboration weight that goes to genre b.
    """
    from matplotlib.colors import LinearSegmentedColormap

    _style_figure()
    wide = matrix.pivot(index="genre_a", columns="genre_b", values="weight").reindex(
        index=genres, columns=genres
    )
    order = wide.sum(axis=1).sort_values(ascending=False, kind="mergesort").index
    wide = wide.loc[order, order]
    rows = wide.div(wide.sum(axis=1).replace(0, np.nan), axis=0)
    cmap = LinearSegmentedColormap.from_list("blue", ["#f4f8fd", *BLUE_RAMP])
    fig, ax = plt.subplots(figsize=(8.6, 7.4))
    vmax = float(np.nanmax(rows.values))
    im = ax.imshow(rows.values, cmap=cmap, vmin=0, vmax=vmax)
    ax.grid(False)
    ax.set_xticks(range(len(order)), order, rotation=45, ha="right")
    ax.set_yticks(range(len(order)), order)
    for i in range(len(order)):
        for j in range(len(order)):
            v = rows.values[i, j]
            if np.isfinite(v) and v >= 0.005:
                ax.text(
                    j,
                    i,
                    f"{v:.0%}",
                    ha="center",
                    va="center",
                    fontsize=6.5,
                    color="white" if v > 0.55 * vmax else TEXT,
                )
    bar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02, format=lambda v, _p: f"{v:.0%}")
    bar.outline.set_visible(False)
    ax.set_title("Share of each genre's labelled collaboration weight, by partner genre")
    ax.set_ylabel("genre (row sums to 100 %)")
    ax.set_xlabel(f"partner genre · cells under 0.5 % left blank · graph {graph}")
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(target, dpi=DPI)
    plt.close(fig)
    return target
