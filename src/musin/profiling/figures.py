"""Static report figures for the profiling report (F1.4).

Reads the profile_*.csv tables (already aggregated) and writes PNGs. Style
follows the project's chart conventions: light surface, one hue per series in
fixed order (blue, then orange), thin marks, recessive grid, direct labels.
"""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"
SERIES = ("#2a78d6", "#eb6834")  # categorical slots 1, 2
DPI = 160


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "axes.edgecolor": GRID,
            "axes.labelcolor": TEXT_2,
            "axes.titlecolor": TEXT,
            "axes.titlesize": 11,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": TEXT_2,
            "ytick.color": TEXT_2,
            "font.size": 9,
            "legend.frameon": False,
            "legend.labelcolor": TEXT,
        }
    )


def _read(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _thousands(x, _pos) -> str:
    return f"{x:,.0f}"


def credit_sizes(rows: list[dict], max_size: int, out: Path) -> None:
    labels = [r["size_bucket"] for r in rows]
    x = range(len(rows))
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(8, 5.6), sharex=True)
    top.bar(x, [int(r["recordings"]) for r in rows], width=0.8, color=SERIES[0])
    top.set_yscale("log")
    top.set_title("Recordings by number of credited artists")
    top.set_ylabel("recordings (log scale)")
    top.grid(axis="x", visible=False)
    bottom.bar(x, [float(r["pair_share"]) for r in rows], width=0.8, color=SERIES[0])
    bottom.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    bottom.set_title("Share of all artist pairs (n·(n−1)/2 per recording)")
    bottom.set_ylabel("share of pairs")
    bottom.set_xticks(list(x), labels)
    bottom.set_xlabel("credited artists per recording")
    bottom.grid(axis="x", visible=False)
    if max_size in [int(r["size_order"]) for r in rows]:
        pos = [int(r["size_order"]) for r in rows].index(max_size) + 0.5
        for ax in (top, bottom):
            ax.axvline(pos, color=TEXT_2, linewidth=1, linestyle="--")
        bottom.annotate(
            f"max_credit_size = {max_size}",
            xy=(pos, bottom.get_ylim()[1] * 0.9),
            xytext=(4, 0),
            textcoords="offset points",
            color=TEXT_2,
        )
    fig.tight_layout()
    fig.savefig(out, dpi=DPI)
    plt.close(fig)


def years_by_decade(rows: list[dict], out: Path) -> None:
    rows = [r for r in rows if 1900 <= int(r["decade"]) <= 2020]
    labels = [f"{r['decade']}s" for r in rows]
    x = list(range(len(rows)))
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(8, 5.6), sharex=True)
    top.bar(x, [int(r["recordings"]) for r in rows], width=0.8, color=SERIES[0])
    top.yaxis.set_major_formatter(FuncFormatter(_thousands))
    top.set_title("Recordings by decade of first release")
    top.set_ylabel("recordings")
    top.grid(axis="x", visible=False)
    series = [
        ("producer credit", [float(r["share_produced_for"]) for r in rows]),
        ("performer credit", [float(r["share_performed_for"]) for r in rows]),
    ]
    # Direct labels at the line ends; the higher line's label is nudged up and
    # the lower one's down, so close end values never collide.
    end = x[-1]
    top_end = max(s[1][end] for s in series)
    for i, (name, ys) in enumerate(series):
        bottom.plot(x, ys, color=SERIES[i], linewidth=2, marker="o", markersize=4, label=name)
        bottom.annotate(
            name,
            (end, ys[end]),
            xytext=(8, 5 if ys[end] >= top_end else -5),
            textcoords="offset points",
            va="center",
            color=TEXT,
        )
    bottom.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    bottom.set_ylim(0, max(max(ys) for _, ys in series) * 1.15)
    for ax in (top, bottom):
        ax.set_xlim(-0.6, end + 1.9)
    bottom.set_title("Share of recordings with a contribution credit (recording level)")
    bottom.set_ylabel("share of recordings")
    bottom.set_xticks(x, labels, rotation=45)
    bottom.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(out, dpi=DPI)
    plt.close(fig)


COVERAGE = (
    ("years", "share of recordings dated", "Recordings with a first release year"),
    ("featuring", "share of recordings", "Recordings with a featured artist"),
    (
        "contributions",
        "share with produced_for (recording level)",
        "Recordings with a producer credit",
    ),
    (
        "contributions",
        "share with performed_for (recording level)",
        "Recordings with a performer credit",
    ),
    ("tags", "share (artist level)", "Credited artists with a genre tag (artist tags)"),
    (
        "tags",
        "share (either source)",
        "Credited artists with a genre tag (artist or release group)",
    ),
    (
        "tags",
        "appearance-weighted share (either source)",
        "Artist appearances with a genre tag (either)",
    ),
)


def coverage(summary: list[dict], out: Path) -> None:
    lookup = {(s["section"], s["metric"]): s["value"] for s in summary}
    items = [
        (label, float(lookup[(sec, met)])) for sec, met, label in COVERAGE if (sec, met) in lookup
    ]
    fig, ax = plt.subplots(figsize=(8, 0.45 * len(items) + 1.0))
    y = list(range(len(items)))[::-1]
    ax.barh(y, [v for _, v in items], height=0.6, color=SERIES[0])
    for yi, (_, v) in zip(y, items, strict=True):
        ax.annotate(
            f"{v:.1%}", (v, yi), xytext=(4, 0), textcoords="offset points", va="center", color=TEXT
        )
    ax.set_yticks(y, [label for label, _ in items])
    ax.set_xlim(0, 1.08)
    ax.xaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.grid(axis="y", visible=False)
    fig.suptitle(
        "Coverage of the fields the research questions depend on",
        x=0.01,
        ha="left",
        fontsize=11,
        fontweight="bold",
        color=TEXT,
    )
    fig.tight_layout()
    fig.savefig(out, dpi=DPI)
    plt.close(fig)


def render_all(tables: Path, figures: Path, summary: list[dict], max_size: int) -> list[Path]:
    """Write every profiling figure; returns the paths written."""
    _style()
    outs = [
        figures / "profile_credit_sizes.png",
        figures / "profile_years_by_decade.png",
        figures / "profile_coverage.png",
    ]
    credit_sizes(_read(tables / "profile_credit_sizes.csv"), max_size, outs[0])
    years_by_decade(_read(tables / "profile_years_by_decade.csv"), outs[1])
    coverage(summary, outs[2])
    return outs
