"""Report figures that the P3–P5 stages don't draw themselves (F6.1, FR-24).

Every figure reads aggregated tables from reports/tables only, so the whole
set redraws in seconds without Spark or Neo4j. Style is the project's chart
style (musin.profiling.figures) with the CJK font fallback of F4.3. Colours
follow the validated reference palette:

- categorical slots in fixed order (blue, orange, aqua, yellow, magenta,
  green) for stacked bars, where only neighbouring segments must differ;
- at most the first three slots where marks of every colour sit side by side
  (dot plots), plus a distinct marker shape per series;
- a legend for every multi-series chart and direct labels where they fit, so
  identity never rests on colour alone; text stays in text colours.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from musin.analytics.bridge_figures import FONT_FAMILY
from musin.profiling.figures import DPI, GRID, SURFACE, TEXT, TEXT_2, _style, plt

# Reference palette, light mode, categorical slots 1-6 (all-pairs safe: 1-3 only).
SLOTS = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300")
REST = "#c9c8c3"  # neutral for "everything else" segments
MARKERS = ("o", "s", "^")

ENGINE_LABELS = {
    "gds": "Neo4j GDS",
    "graphframes": "Spark GraphFrames",
    "cugraph": "RAPIDS cuGraph",
}
MEASURE_LABELS = {
    "degreeWeighted": "weighted degree",
    "pagerank": "PageRank",
    "betweenness": "betweenness",
    "bridgeScore": "bridge score",
    "genreDiversity": "genre diversity",
}
PROJECTION_LABELS = {"collab_undirected": "COLLAB (all four types)", "credit_only": "credit-only"}
FATE_LABELS = {
    "kept": "kept",
    "self_loop": "self-loop",
    "excluded_classical": "classical artist",
    "band_member": "group ↔ own member",
    "same_recording_pair": "pair already on the recording",
    "excluded_artist_type": "Character artist",
}
EDGE_TYPE_LABELS = {
    "co_credited_weight": "CO_CREDITED",
    "featured_weight": "FEATURED",
    "produced_for_weight": "PRODUCED_FOR",
    "performed_for_weight": "PERFORMED_FOR",
}


def _setup() -> None:
    _style()
    plt.rcParams["font.family"] = FONT_FAMILY


def _save(fig, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(target, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    return target


def _footer(fig, text: str, y: float = -0.02) -> None:
    """Source line under the figure; `y` in figure coordinates (lower it below a legend)."""
    fig.text(0.01, y, text, fontsize=7, color=TEXT_2, ha="left", va="top")


def _seconds_label(v: float) -> str:
    return f"{v:.2f} s" if v < 1 else (f"{v:.1f} s" if v < 100 else f"{v:,.0f} s")


def benchmark_pagerank(summary: pd.DataFrame, agreement: pd.DataFrame, target: Path) -> Path:
    """Dot plot of PageRank time per phase and engine (median, min–max), log scale (F5.2/F5.3).

    In: benchmark_pagerank_summary.csv (engine, <phase>_{median,min,max}_s)
    and benchmark_pagerank_agreement.csv (a, b, spearman_rho, top_k,
    jaccard_top_k) for the footer line on agreement with GDS.
    """
    _setup()
    engines = [e for e in ENGINE_LABELS if e in set(summary["engine"])]
    phases = [
        ("load", "load graph"),
        ("compute", "compute"),
        ("output", "write scores"),
        ("total", "end to end"),
    ]
    by_engine = summary.set_index("engine")
    fig, ax = plt.subplots(figsize=(8.0, 4.4))
    step = 0.24
    yticks, ylabels = [], []
    for i, (phase, label) in enumerate(phases):
        base = (len(phases) - 1 - i) * (len(engines) * step + 0.45)
        yticks.append(base + step * (len(engines) - 1) / 2)
        ylabels.append(label)
        for j, engine in enumerate(engines):
            row = by_engine.loc[engine]
            y = base + step * (len(engines) - 1 - j)
            med = row[f"{phase}_median_s"]
            ax.hlines(y, row[f"{phase}_min_s"], row[f"{phase}_max_s"], color=SLOTS[j], lw=1.2)
            ax.plot(
                med,
                y,
                MARKERS[j],
                color=SLOTS[j],
                ms=8,
                mec=SURFACE,
                mew=1.5,
                label=ENGINE_LABELS[engine] if i == 0 else None,
            )
            ax.annotate(
                _seconds_label(med),
                (row[f"{phase}_max_s"], y),
                xytext=(6, 0),
                textcoords="offset points",
                va="center",
                fontsize=7.5,
                color=TEXT_2,
            )
    ax.set_xscale("log")
    ax.set_xlim(
        left=min(summary[[f"{p}_min_s" for p, _ in phases]].min()) / 2,
        right=max(summary[[f"{p}_max_s" for p, _ in phases]].max()) * 4,
    )
    ax.xaxis.set_major_formatter(lambda v, _p: f"{v:g} s")
    ax.set_yticks(yticks, ylabels)
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="y", visible=False)
    ax.set_title("PageRank on the same graph: time per phase (median of 3, line: min–max)")
    ax.set_xlabel("seconds (log scale)")
    ax.legend(loc="upper right", fontsize=8)  # the load row ends well left of it
    vs_gds = agreement[(agreement["a"] == "gds") & agreement["b"].isin(engines)]
    parts = [
        f"{ENGINE_LABELS[r.b]}: top-{r.top_k} Jaccard {r.jaccard_top_k:.2f}, "
        f"Spearman ρ {r.spearman_rho:.5f}"
        for r in vs_gds.itertuples()
    ]
    graph = summary["graph"].iloc[0]
    _footer(
        fig,
        "Agreement with GDS · "
        + " · ".join(parts)
        + f"\nunweighted PageRank on the COLLAB pairs · graph {graph}",
    )
    return _save(fig, target)


def centrality_agreement(agreement: pd.DataFrame, target: Path) -> Path:
    """Spearman ρ and top-k Jaccard per pair of centrality measures, per projection (F4.1, RQ4).

    In: centrality_agreement.csv (comparison, measure_a, measure_b,
    spearman_rho, top_k, jaccard_top_k). Rows comparing two projections are
    left out; they belong to the sensitivity discussion.
    """
    _setup()
    within = agreement[agreement["comparison"].isin(PROJECTION_LABELS)]
    projections = [p for p in PROJECTION_LABELS if p in set(within["comparison"])]
    k = int(within["top_k"].iloc[0])
    fig, axes = plt.subplots(1, len(projections), figsize=(9.2, 3.6), sharey=True)
    axes = np.atleast_1d(axes)
    width = 0.36
    for ax, projection in zip(axes, projections, strict=True):
        rows = within[within["comparison"] == projection].reset_index(drop=True)
        x = np.arange(len(rows))
        labels = [
            f"{MEASURE_LABELS[a]}\nvs {MEASURE_LABELS[b]}"
            for a, b in zip(rows["measure_a"], rows["measure_b"], strict=True)
        ]
        series = [
            ("spearman_rho", "Spearman ρ, all artists"),
            ("jaccard_top_k", f"Jaccard of the top-{k} sets"),
        ]
        for s, (col, label) in enumerate(series):
            xs = x + (s - 0.5) * width
            ax.bar(
                xs,
                rows[col],
                width=width * 0.94,
                color=SLOTS[s],
                label=label,
                edgecolor=SURFACE,
                linewidth=1,
            )
            for xi, v in zip(xs, rows[col], strict=True):
                ax.annotate(
                    f"{v:.2f}",
                    (xi, v),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    fontsize=7.5,
                    color=TEXT_2,
                )
        ax.set_xticks(x, labels, fontsize=8)
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, 1.05)
        ax.set_title(PROJECTION_LABELS[projection])
    axes[0].set_ylabel("agreement (0 = none, 1 = identical)")
    axes[0].legend(loc="upper right", fontsize=8)
    fig.suptitle(
        "Centrality measures agree overall but not on who is at the top",
        x=0.01,
        ha="left",
        fontsize=11,
        fontweight="bold",
        color=TEXT,
    )
    _footer(fig, f"graph {within['graph'].iloc[0]} · F4.1 centrality_agreement.csv")
    fig.tight_layout()
    return _save(fig, target)


def _bucket_label(bucket: str) -> str:
    """'1000-9999' -> '1,000–9,999'."""
    lo, _, hi = bucket.partition("-")
    return f"{int(lo):,}–{int(hi):,}" if hi else bucket


def communities_sizes(sizes: pd.DataFrame, runs: pd.DataFrame, target: Path) -> Path:
    """Community count and artist share per size bucket (F4.2, RQ2).

    In: communities_sizes.csv (bucket, communities, artist_share) and the
    run of record in communities_runs.csv for the footer.
    """
    _setup()
    fig, (left, right) = plt.subplots(1, 2, figsize=(9.2, 3.4))
    x = np.arange(len(sizes))
    left.bar(x, sizes["communities"], color=SLOTS[0], width=0.75)
    left.set_yscale("log")
    left.set_title("Communities per size bucket")
    left.set_ylabel("communities (log scale)")
    right.bar(x, sizes["artist_share"] * 100, color=SLOTS[0], width=0.75)
    right.set_title("Share of all artists in each bucket")
    right.yaxis.set_major_formatter(lambda v, _p: f"{v:.0f}%")
    for ax, col, fmt in ((left, "communities", "{:,.0f}"), (right, "artist_share", "{:.0%}")):
        vals = sizes[col] * (100 if col == "artist_share" else 1)
        for xi, v, raw in zip(x, vals, sizes[col], strict=True):
            ax.annotate(
                fmt.format(raw),
                (xi, v),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                fontsize=7.5,
                color=TEXT_2,
            )
        ax.set_xticks(x, [_bucket_label(b) for b in sizes["bucket"]], rotation=30, ha="right")
        ax.set_xlabel("artists per community")
        ax.grid(axis="x", visible=False)
    rec = runs[runs["run"] == "record"].iloc[0]
    _footer(
        fig,
        f"Louvain run of record: {int(rec['communities']):,} communities, modularity "
        f"{rec['modularity']:.3f} · graph {rec['graph']}",
    )
    fig.tight_layout()
    return _save(fig, target)


def communities_composition(top: pd.DataFrame, target: Path) -> Path:
    """Genre mix of the largest communities: top two genres and the rest (F4.2, RQ2).

    In: communities_top.csv (community, size, labelled_share, share_<genre>,
    example_artists). Fifteen genres can't each get a colour, so every bar
    shows its own two leading genres (blue, orange, named in the bar) and the
    rest in grey; shares are of the community's labelled artists.
    """
    _setup()
    share_cols = [c for c in top.columns if c.startswith("share_")]
    fig, ax = plt.subplots(figsize=(9.6, 0.52 * len(top) + 1.4))
    y = np.arange(len(top))[::-1]
    for yi, (_, row) in zip(y, top.iterrows(), strict=True):
        # Genre columns contain spaces and '&', so read rows as Series, not namedtuples.
        shares = pd.Series({c[len("share_") :]: row[c] for c in share_cols}).astype(float)
        shares = shares.sort_values(ascending=False, kind="mergesort")
        parts = [
            (shares.index[0], shares.iloc[0], SLOTS[0]),
            (shares.index[1], shares.iloc[1], SLOTS[1]),
            ("other genres", 1 - shares.iloc[0] - shares.iloc[1], REST),
        ]
        left = 0.0
        unnamed = []  # genres whose segment is too narrow for its label
        for name, v, colour in parts:
            ax.barh(
                yi,
                v * 100,
                left=left * 100,
                color=colour,
                height=0.7,
                edgecolor=SURFACE,
                linewidth=1.5,
            )
            text = f"{name} {v:.0%}"
            if v * 100 >= 0.95 * len(text) + 2:  # ~0.9 axis units per character at 7.5 pt
                ink = "white" if colour != REST else TEXT
                ax.text(
                    (left + v / 2) * 100,
                    yi,
                    text,
                    ha="center",
                    va="center",
                    fontsize=7.5,
                    color=ink,
                )
            elif colour == SLOTS[1]:
                unnamed.append(f"2nd: {text}")
            left += v
        examples = "; ".join(row["example_artists"].split("; ")[:3])
        lead = " · ".join([f"{row['labelled_share']:.0%} labelled", *unnamed])
        ax.text(101.5, yi, f"{lead} · {examples}", va="center", fontsize=7.5, color=TEXT_2)
    ax.set_yticks(
        y, [f"#{c} · {s:,} artists" for c, s in zip(top["community"], top["size"], strict=True)]
    )
    ax.tick_params(axis="y", length=0)
    ax.set_xlim(0, 100)
    ax.xaxis.set_major_formatter(lambda v, _p: f"{v:.0f}%")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel(
        f"share of the community's labelled artists (those with a primary genre) · "
        f"graph {top['graph'].iloc[0]}"
    )
    ax.text(
        101.5,
        y[0] + 0.75,
        "labelled share · example artists",
        fontsize=7.5,
        color=TEXT_2,
        fontweight="bold",
        va="center",
    )
    ax.set_title(f"Genre mix of the {len(top)} largest Louvain communities")
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in (SLOTS[0], SLOTS[1], REST)]
    ax.legend(
        handles,
        ["largest genre", "second genre", "all other genres"],
        fontsize=8,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.16),
        ncol=3,
    )
    return _save(fig, target)


def validation_ranks(ranks: pd.DataFrame, summary: pd.DataFrame, target: Path) -> Path:
    """Where the pre-registered artists rank, as the top share of each measure (F4.6, RQ1).

    In: validation_ranks.csv (listed_name, in_graph, <measure>_rank) and
    validation_summary.csv (measure, artists_with_measure). Top share =
    rank / artists with the measure, so measures over different populations
    (126 k artists with a bridge score, 1.1 M with betweenness) share one axis.
    """
    _setup()
    measures = ["bridgeScore", "betweenness", "pagerank"]
    n = summary.set_index("measure")["artists_with_measure"]
    shown = ranks[ranks["in_graph"].astype(bool)].copy()
    for m in measures:
        shown[f"{m}_top"] = shown[f"{m}_rank"] / n[m]
    shown = shown.sort_values("bridgeScore_top", ascending=False, kind="mergesort")
    missing = ranks.loc[~ranks["in_graph"].astype(bool), "listed_name"].tolist()
    fig, ax = plt.subplots(figsize=(8.4, 0.34 * len(shown) + 1.6))
    y = np.arange(len(shown))
    for i, m in enumerate(measures):
        ax.plot(
            shown[f"{m}_top"],
            y,
            MARKERS[i],
            color=SLOTS[i],
            ms=8,
            mec=SURFACE,
            mew=1.2,
            label=f"{MEASURE_LABELS[m]} (of {int(n[m]):,} artists)",
            linestyle="none",
        )
    ax.set_xscale("log")
    # left > right: the top ranks sit on the right.
    ax.set_xlim(1.2, min(shown[[f"{m}_top" for m in measures]].min()) / 2)
    ax.xaxis.set_major_formatter(lambda v, _p: f"top {v * 100:g}%")
    ax.set_yticks(y, shown["listed_name"])
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.5)
    ax.set_xlabel("rank as a share of all artists with the measure (log scale; right = higher)")
    ax.set_title("Pre-registered cross-genre artists: rank on each measure")
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9, frameon=True, edgecolor=GRID)
    note = f"not in the graph: {', '.join(missing)} · " if missing else ""
    _footer(fig, f"{note}graph {ranks['graph'].iloc[0]} · F4.6 validation_ranks.csv")
    return _save(fig, target)


def trends_edge_types(trends: pd.DataFrame, target: Path) -> Path:
    """Edge-type mix and genre coverage by decade (F4.4, RQ3 context).

    In: trends_by_decade.csv (decade, total_weight, <type>_weight,
    labelled_share). Left: each edge type's share of the decade's weight
    (stacked to 100 %). Right: the share of weight whose two endpoints have a
    primary genre, which bounds what the cross-genre share can say.
    """
    _setup()
    fig, (left, right) = plt.subplots(
        1, 2, figsize=(10.0, 3.8), gridspec_kw={"width_ratios": [1.45, 1]}
    )
    labels = [f"{d}s" for d in trends["decade"]]
    x = np.arange(len(trends))
    bottom = np.zeros(len(trends))
    for i, (col, name) in enumerate(EDGE_TYPE_LABELS.items()):
        share = (trends[col] / trends["total_weight"]).to_numpy() * 100
        left.bar(
            x,
            share,
            bottom=bottom,
            color=SLOTS[i],
            width=0.78,
            label=name,
            edgecolor=SURFACE,
            linewidth=1.5,
        )
        last = len(x) - 1
        if share[last] >= 8:
            left.text(
                x[last],
                bottom[last] + share[last] / 2,
                f"{share[last]:.0f}%",
                ha="center",
                va="center",
                fontsize=7,
                color="white" if i < 2 else TEXT,
            )
        bottom += share
    left.set_xticks(x, labels)
    left.set_ylim(0, 100)
    left.yaxis.set_major_formatter(lambda v, _p: f"{v:.0f}%")
    left.grid(axis="x", visible=False)
    left.set_title("Collaboration weight by edge type")
    left.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=4, fontsize=7.5)
    right.plot(labels, trends["labelled_share"] * 100, color=SLOTS[0], lw=2, marker="o", ms=5)
    for lab, v in zip(labels, trends["labelled_share"], strict=True):
        right.annotate(
            f"{v:.0%}",
            (lab, v * 100),
            xytext=(0, 6),
            textcoords="offset points",
            ha="center",
            fontsize=7,
            color=TEXT_2,
        )
    right.set_ylim(0, 100)
    right.yaxis.set_major_formatter(lambda v, _p: f"{v:.0f}%")
    right.set_title("Weight with a primary genre at both ends")
    fig.suptitle(
        "Who collaborates how, and how much of it the genre analysis can see",
        x=0.01,
        ha="left",
        fontsize=11,
        fontweight="bold",
        color=TEXT,
    )
    _footer(
        fig,
        f"decade of first release · graph {trends['graph'].iloc[0]} · F4.4 trends_by_decade.csv",
    )
    fig.tight_layout()
    return _save(fig, target)


def graph_build_instances(fates: pd.DataFrame, funnel: pd.DataFrame, target: Path) -> Path:
    """What F3.1 did with every edge instance, per edge type (stacked horizontal bars).

    In: graph_build_instances.csv (edge_type, fate, instances) and
    pipeline_funnel.csv for the footer (credit-size cap, resulting edges).
    """
    _setup()
    types = list(dict.fromkeys(fates["edge_type"]))
    fate_order = list(FATE_LABELS)
    fig, ax = plt.subplots(figsize=(9.6, 3.6))
    y = np.arange(len(types))[::-1]
    for yi, edge_type in zip(y, types, strict=True):
        rows = fates[fates["edge_type"] == edge_type].set_index("fate")
        left = 0.0
        for i, fate in enumerate(fate_order):
            v = rows.loc[fate, "instances"] / 1e6
            ax.barh(
                yi,
                v,
                left=left,
                color=SLOTS[i],
                height=0.66,
                edgecolor=SURFACE,
                linewidth=1.5,
                label=FATE_LABELS[fate] if yi == y[0] else None,
            )
            if v >= 0.9:
                ax.text(
                    left + v / 2,
                    yi,
                    f"{v:.1f} M",
                    ha="center",
                    va="center",
                    fontsize=7,
                    color="white" if i < 2 else TEXT,
                )
            left += v
        ax.annotate(
            f"{left:.1f} M",
            (left, yi),
            xytext=(5, 0),
            textcoords="offset points",
            va="center",
            fontsize=7.5,
            color=TEXT_2,
        )
    ax.set_yticks(y, types)
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("edge instances (millions; one per recording, artist pair and type)")
    ax.set_title("F3.1 graph build: kept and dropped edge instances by type")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=3, fontsize=8)
    value = dict(zip(funnel["quantity"], funnel["value"], strict=True))
    _footer(
        fig,
        f"Before pairing, {value['recordings over the credit-size cap (no edges)']:,} "
        f"recordings over the credit-size cap were skipped "
        f"({value['pair instances skipped by the credit-size cap']:,} pair instances). "
        f"Kept instances aggregate to {value['edges (artist pair × type)']:,} edges "
        f"· graph {funnel['graph'].iloc[0]}",
        y=-0.21,
    )
    return _save(fig, target)
