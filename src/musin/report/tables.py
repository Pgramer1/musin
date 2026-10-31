"""Report-ready tables (F6.1, FR-24): selected columns, rounded, as Markdown and LaTeX.

Each builder takes the CSVs it needs (already in reports/tables) and returns a
ReportTable. The report quotes these files, so no number in it is typed by
hand (RULES §9). Builders only select, rename, round and order; every metric
was computed by the stage that wrote the source CSV.
"""

from __future__ import annotations

import pandas as pd

from musin.report.derived import FATES
from musin.report.figures import ENGINE_LABELS, FATE_LABELS, MEASURE_LABELS, PROJECTION_LABELS
from musin.report.formats import ReportTable, fmt_float, fmt_int, fmt_pct, fmt_seconds

BETWEENNESS_NOTE = (
    "Sampled betweenness is reproducible within one import but not across re-imports of the "
    "same graph (P4 §7): quote ranks and percentiles, not the exact top-20 order."
)
SPOKEN_WORD_NOTE = (
    "Spoken-word recordings (audiobooks) are still in the graph (D-040, open G1 item), and "
    "the F4.6 spot-check verdicts on these lists still need review."
)


def _graph_note(frame: pd.DataFrame) -> str:
    fp = (
        f", edges fingerprint {frame['edges_fingerprint'].iloc[0]}"
        if ("edges_fingerprint" in frame.columns)
        else ""
    )
    return f"Graph {frame['graph'].iloc[0]}{fp}."


def _measure(m: str) -> str:
    return MEASURE_LABELS.get(m, m)


def data_funnel(funnel: pd.DataFrame) -> ReportTable:
    rows = [
        [str(r.step), r.layer, r.quantity, fmt_int(r.value), f"{r.source_table}: {r.source_metric}"]
        for r in funnel.itertuples()
    ]
    return ReportTable(
        "data_funnel",
        "From the MusicBrainz dump to the loaded graph",
        ["Step", "Layer", "Quantity", "Value", "Source"],
        "rllrl",
        rows,
        ["pipeline_funnel.csv"],
        [_graph_note(funnel)],
    )


def data_genres(genres: pd.DataFrame) -> ReportTable:
    g = genres.sort_values("credited_artists_primary", ascending=False, kind="mergesort")
    rows = [
        [
            r.parent_genre,
            fmt_int(r.official_genres_mapped),
            fmt_int(r.tag_votes),
            fmt_int(r.credited_artists_primary),
            fmt_pct(r.credited_share),
        ]
        for r in g.itertuples()
    ]
    return ReportTable(
        "data_genres",
        "The 15 parent genres (F2.5)",
        ["Parent genre", "Official genres", "Tag votes", "Credited artists", "Share"],
        "lrrrr",
        rows,
        ["genre_parent_summary.csv"],
        [
            "Credited artists whose primary genre this is, and their share of all credited "
            "artists (clean layer, before the graph build; classical is excluded from the main "
            "graph later, D-039)."
        ],
    )


def graph_instances(fates: pd.DataFrame) -> ReportTable:
    rows = []
    for edge_type, part in fates.groupby("edge_type", sort=False):
        by = part.set_index("fate")
        total = by["instances"].sum()
        rows.append(
            [
                edge_type,
                *[fmt_int(by.loc[f, "instances"]) for f in FATES],
                fmt_pct(by.loc["kept", "instances"] / total),
            ]
        )
    return ReportTable(
        "graph_instances",
        "Edge instances kept and dropped by the graph build (F3.1)",
        ["Edge type", *[FATE_LABELS[f] for f in FATES], "Kept share"],
        "l" + "r" * (len(FATES) + 1),
        rows,
        ["graph_instances.csv"],
        [
            "One instance per recording, artist pair and type; each dropped instance is counted "
            "under the first rule that removes it.",
            _graph_note(fates),
        ],
    )


def data_quality(checks: pd.DataFrame) -> ReportTable:
    counts = checks.groupby(["table", "status"]).size().unstack(fill_value=0)
    statuses = [s for s in ("pass", "accepted", "fail") if s in counts.columns]
    rows = [[t, *[fmt_int(counts.loc[t, s]) for s in statuses]] for t in counts.index]
    rows.append(["all", *[fmt_int(counts[s].sum()) for s in statuses]])
    return ReportTable(
        "data_quality",
        "Graph-layer quality checks after the final build (F3.3)",
        ["Table", *statuses],
        "l" + "r" * len(statuses),
        rows,
        ["graph_checks.csv"],
        [
            "'accepted': a run-to-run row-count change beyond checks.max_row_change, reviewed and "
            "accepted after the G1 rebuild (D-039)."
        ],
    )


def centrality_top(top: pd.DataFrame, projection: str) -> ReportTable:
    part = top[top["projection"] == projection]
    k = int(part["rank"].max())
    measures = ["degreeWeighted", "pagerank", "betweenness"]

    def cell(m: str, rank: int) -> str:
        r = part[(part["measure"] == m) & (part["rank"] == rank)]
        if r.empty:
            return "–"
        genre = r["primary_genre"].iloc[0]
        return r["name"].iloc[0] + (f" ({genre})" if isinstance(genre, str) else "")

    rows = [[str(rank), *[cell(m, rank) for m in measures]] for rank in range(1, k + 1)]
    return ReportTable(
        f"rq4_centrality_top_{projection}",
        f"Top {k} artists per centrality measure, {PROJECTION_LABELS[projection]} "
        "projection (F4.1)",
        ["Rank", *[_measure(m) for m in measures]],
        "rlll",
        rows,
        ["centrality_top.csv"],
        ["Primary genre in brackets.", BETWEENNESS_NOTE, SPOKEN_WORD_NOTE, _graph_note(part)],
    )


def centrality_agreement(agree: pd.DataFrame) -> ReportTable:
    rows = [
        [
            PROJECTION_LABELS.get(r.comparison, r.comparison),
            f"{_measure(r.measure_a)} vs {_measure(r.measure_b)}",
            fmt_int(r.n_artists),
            fmt_float(r.spearman_rho, 3),
            fmt_float(r.jaccard_top_k, 3),
        ]
        for r in agree.itertuples()
    ]
    k = int(agree["top_k"].iloc[0])
    return ReportTable(
        "rq4_agreement",
        "Agreement between centrality measures and between projections (F4.1)",
        ["Projection", "Measures", "Artists", "Spearman ρ", f"Top-{k} Jaccard"],
        "llrrr",
        rows,
        ["centrality_agreement.csv"],
        [
            "Rows 'collab_undirected vs credit_only' compare one measure across the two "
            "projections, over the artists in both.",
            _graph_note(agree),
        ],
    )


def centrality_runs(runs: pd.DataFrame) -> ReportTable:
    rows = [
        [
            PROJECTION_LABELS[r.projection],
            _measure(r.measure),
            fmt_int(r.nodes),
            fmt_int(r.relationships),
            fmt_seconds(r.compute_millis / 1000),
            fmt_int(r.ran_iterations) if r.measure == "pagerank" else "–",
        ]
        for r in runs.itertuples()
    ]
    first = runs.iloc[0]
    return ReportTable(
        "rq4_runs",
        "Centrality runs and their settings (F4.1)",
        ["Projection", "Measure", "Nodes", "Relationships", "Compute", "Iterations"],
        "llrrrr",
        rows,
        ["centrality_runs.csv"],
        [
            f"PageRank: damping {first['damping']}, at most {int(first['max_iterations'])} "
            f"iterations, converged at GDS's tolerance 1e-7. Betweenness: unweighted, "
            f"{int(first['sampling_size']):,} sampled sources, seed {int(first['sampling_seed'])}, "
            f"concurrency {int(first['betweenness_concurrency'])}; others at concurrency "
            f"{int(first['concurrency'])}. Relationships count both directions of each pair.",
            _graph_note(runs),
        ],
    )


def bridges_top(top: pd.DataFrame) -> ReportTable:
    rows = [
        [
            str(r.bridge_rank),
            r.name,
            r.type if isinstance(r.type, str) else "–",
            r.primary_genre if isinstance(r.primary_genre, str) else "–",
            fmt_float(r.genre_diversity, 3),
            fmt_int(r.labelled_neighbours),
            fmt_int(r.neighbour_genres),
            r.genre_profile,
            fmt_pct(r.betweenness_pct, 2),
            fmt_float(r.bridge_score, 3),
        ]
        for r in top.itertuples()
    ]
    return ReportTable(
        "rq1_bridges_top",
        f"Top {len(top)} genre bridges (F4.3)",
        [
            "Rank",
            "Artist",
            "Type",
            "Primary genre",
            "Diversity",
            "Labelled nbrs",
            "Nbr genres",
            "Largest neighbour genres",
            "Betweenness pct",
            "Bridge score",
        ],
        "rllllrrlrr",
        rows,
        ["bridges_top.csv"],
        [
            "Bridge score = genre diversity × betweenness percentile (PRD §5 RQ1). Labels credited "
            "as artists (e.g. Putumayo) are not filtered (P4 §7).",
            _graph_note(top),
        ],
    )


def bridges_summary(summary: pd.DataFrame) -> ReportTable:
    s = summary.iloc[0]
    items = [
        ("Artists in the graph", fmt_int(s["artists"])),
        ("… with at least one labelled neighbour", fmt_int(s["with_labelled_neighbour"])),
        (
            f"… with a defined diversity (≥ {int(s['min_labelled_neighbours'])} labelled "
            "neighbours)",
            fmt_int(s["with_diversity"]),
        ),
        (
            "Diversity 25th / 50th / 75th / 90th percentile",
            " / ".join(fmt_float(s[f"diversity_p{p}"], 2) for p in (25, 50, 75, 90)),
        ),
        ("Artists with diversity 0", fmt_int(s["diversity_zero"])),
        ("Artists with betweenness 0", fmt_pct(s["betweenness_zero_share"])),
        ("Labelled artist pairs", fmt_int(s["labelled_pairs"])),
        ("Labelled weight (shared recordings)", fmt_int(s["labelled_weight"])),
        ("Share of labelled weight within one genre", fmt_pct(s["within_genre_weight_share"])),
    ]
    return ReportTable(
        "rq1_bridges_summary",
        "Genre diversity and labelled collaboration (F4.3)",
        ["Quantity", "Value"],
        "lr",
        [list(i) for i in items],
        ["bridges_summary.csv"],
        [_graph_note(summary)],
    )


def validation(ranks: pd.DataFrame, summary: pd.DataFrame) -> ReportTable:
    n = summary.set_index("measure")["artists_with_measure"]
    measures = ["bridgeScore", "betweenness", "pagerank", "degreeWeighted"]
    rows = []
    for r in ranks.itertuples():
        if not bool(r.in_graph):
            rows.append([r.listed_name, "not in the graph", *["–"] * (len(measures) + 1)])
            continue
        rows.append(
            [
                r.listed_name,
                r.primary_genre if isinstance(r.primary_genre, str) else "–",
                fmt_pct(r.bridgeScore_pct, 2),
                *[fmt_int(getattr(r, f"{m}_rank")) for m in measures],
            ]
        )
    return ReportTable(
        "rq1_validation",
        "Ranks of the pre-registered cross-genre artists (F4.6)",
        [
            "Artist",
            "Primary genre",
            "Bridge pct",
            *[f"{_measure(m)} rank (of {int(n[m]):,})" for m in measures],
        ],
        "llrrrrr",
        rows,
        ["validation_ranks.csv", "validation_summary.csv"],
        [
            "List committed before any RQ1 result (D-037). Yo‐Yo Ma's primary genre is classical, "
            "so he is not in the main graph (D-039).",
            _graph_note(ranks),
        ],
    )


def communities_runs(runs: pd.DataFrame, stability: pd.DataFrame) -> ReportTable:
    rows = [
        [
            r.run,
            str(r.concurrency),
            fmt_float(r.modularity, 3),
            fmt_int(r.communities),
            fmt_float(r.nmi_genre, 3),
            fmt_float(r.ami_genre, 3),
            fmt_float(r.lcc_nmi_genre, 3),
            fmt_float(r.lcc_ami_genre, 3),
        ]
        for r in runs.itertuples()
    ]
    rec = runs[runs["run"] == "record"].iloc[0]
    return ReportTable(
        "rq2_communities_runs",
        "Louvain runs and agreement with primary genre (F4.2)",
        ["Run", "Threads", "Modularity", "Communities", "NMI", "AMI", "NMI (LCC)", "AMI (LCC)"],
        "lrrrrrrr",
        rows,
        ["communities_runs.csv", "communities_stability.csv"],
        [
            f"NMI/AMI over the {fmt_int(rec['labelled_artists'])} artists with a primary genre; "
            f"LCC = largest connected component ({fmt_int(rec['lcc_artists'])} artists). "
            f"Run-to-run NMI between the four runs: {stability['nmi'].min():.3f}–"
            f"{stability['nmi'].max():.3f} overall, {stability['nmi_lcc'].min():.3f}–"
            f"{stability['nmi_lcc'].max():.3f} inside the LCC.",
            _graph_note(runs),
        ],
    )


def communities_top(top: pd.DataFrame) -> ReportTable:
    share_cols = [c for c in top.columns if c.startswith("share_")]
    rows = []
    for _, r in top.iterrows():  # genre columns contain spaces: rows as Series
        shares = pd.Series({c[len("share_") :]: r[c] for c in share_cols}).astype(float)
        shares = shares.sort_values(ascending=False, kind="mergesort")
        rows.append(
            [
                str(r["community"]),
                fmt_int(r["size"]),
                fmt_pct(r["labelled_share"], 0),
                f"{shares.index[0]} {fmt_pct(shares.iloc[0], 0)}",
                f"{shares.index[1]} {fmt_pct(shares.iloc[1], 0)}",
                r["example_artists"],
            ]
        )
    return ReportTable(
        "rq2_communities_top",
        f"The {len(top)} largest communities (F4.2)",
        ["#", "Artists", "Labelled", "Largest genre", "Second genre", "Example artists"],
        "rrrlll",
        rows,
        ["communities_top.csv"],
        [
            "Genre shares are of the community's labelled artists; example artists are its "
            "highest-PageRank members.",
            _graph_note(top),
        ],
    )


def trends(frame: pd.DataFrame) -> ReportTable:
    rows = [
        [
            f"{r.decade}s",
            fmt_int(r.total_weight),
            fmt_pct(r.labelled_share),
            fmt_pct(r.cross_genre_share),
            fmt_pct(r.featuring_share),
        ]
        for r in frame.itertuples()
    ]
    return ReportTable(
        "rq3_trends",
        "Cross-genre and featuring share by decade (F4.4)",
        ["Decade", "Weight", "Labelled", "Cross-genre share", "Featuring share"],
        "lrrrr",
        rows,
        ["trends_by_decade.csv"],
        [
            "Weight = shared recordings by decade of first release. Cross-genre share is of "
            "labelled weight; featuring share = FEATURED / (FEATURED + CO_CREDITED). Primary genre "
            "is fixed per artist, not per period.",
            _graph_note(frame),
        ],
    )


def sensitivity_lists(frame: pd.DataFrame) -> ReportTable:
    rows = [
        [
            r.variant,
            PROJECTION_LABELS.get(r.projection, r.projection)
            if r.list == "centrality"
            else "bridges",
            _measure(r.measure),
            str(r.k),
            fmt_float(r.jaccard, 2),
            f"{r.common}/{r.k}",
            fmt_int(r.entrants),
            r.admitted_names if isinstance(r.admitted_names, str) and r.admitted_names else "–",
        ]
        for r in frame.itertuples()
    ]
    return ReportTable(
        "sensitivity_top_lists",
        "Top lists of the sensitivity variants against the main graph (F4.7)",
        [
            "Variant",
            "Projection",
            "Measure",
            "k",
            "Jaccard",
            "Common",
            "Entrants",
            "Entrants of the admitted kind (variant rank)",
        ],
        "lllrrrrl",
        rows,
        ["sensitivity_top_lists.csv"],
        [
            "characters keeps Character artists (D-021); classical keeps classical artists "
            "(D-039). " + BETWEENNESS_NOTE,
            f"Main graph {frame['graph_main'].iloc[0]}, edges fingerprint "
            f"{frame['edges_fingerprint_main'].iloc[0]}.",
        ],
    )


def sensitivity_agreement(frame: pd.DataFrame) -> ReportTable:
    rows = [
        [
            r.variant,
            PROJECTION_LABELS.get(r.comparison, r.comparison),
            f"{_measure(r.measure_a)} vs {_measure(r.measure_b)}",
            fmt_float(r.spearman_rho_main, 3),
            fmt_float(r.spearman_rho_variant, 3),
            f"{r.rho_delta:+.3f}",
            fmt_float(r.jaccard_top_k_main, 3),
            fmt_float(r.jaccard_top_k_variant, 3),
            f"{r.jaccard_delta:+.3f}",
        ]
        for r in frame.itertuples()
    ]
    k = int(frame["top_k"].iloc[0])
    return ReportTable(
        "sensitivity_agreement",
        "Centrality agreement in the variants against the main graph (F4.7)",
        [
            "Variant",
            "Projection",
            "Measures",
            "ρ main",
            "ρ variant",
            "Δρ",
            f"Top-{k} J main",
            f"Top-{k} J variant",
            "ΔJ",
        ],
        "lllrrrrrr",
        rows,
        ["sensitivity_agreement.csv"],
        [],
    )


def separation(paths: pd.DataFrame) -> ReportTable:
    rows = [
        [
            r.from_name,
            r.to_name,
            str(r.hops),
            fmt_int(r.shortest_paths),
            r.hop_relations.replace(" | ", "; "),
        ]
        for r in paths.itertuples()
    ]
    return ReportTable(
        "separation_paths",
        "Degrees of separation between example artists (F4.5)",
        ["From", "To", "Hops", "Shortest paths", "Canonical path (type, shared recordings)"],
        "llrrl",
        rows,
        ["separation_paths.csv"],
        [
            "Unweighted hops over the four collaboration types; among tied shortest paths the one "
            "with the smallest artist-id sequence is shown (D-036).",
            _graph_note(paths),
        ],
    )


def bench_scaling(summary: pd.DataFrame) -> ReportTable:
    cores = sorted(summary["cores"].unique())
    rows = []
    for fraction, part in summary.groupby("fraction"):
        by = part.set_index("cores")
        top = by.loc[cores[-1]]
        rows.append(
            [
                fmt_pct(fraction, 0),
                *[fmt_seconds(by.loc[c, "median_s"]) for c in cores],
                fmt_float(top["speedup"], 2),
                fmt_float(top["efficiency"], 2),
            ]
        )
    return ReportTable(
        "bench_scaling",
        "Spark scaling: recording chain runtime, median of 3 (F5.1)",
        [
            "Data",
            *[f"{c} core{'s' if c > 1 else ''}" for c in cores],
            f"Speedup at {cores[-1]}",
            f"Efficiency at {cores[-1]}",
        ],
        "l" + "r" * (len(cores) + 2),
        rows,
        ["benchmark_scaling_summary.csv"],
        [
            "Credits → contributions → years → graph build, fresh process per run, Spark local "
            "mode (D-041). Sample runs still scan the full raw tables, so about a quarter of the "
            "runtime is fixed cost."
        ],
    )


def bench_pagerank(summary: pd.DataFrame, agreement: pd.DataFrame) -> ReportTable:
    vs = agreement[agreement["a"] == "gds"].set_index("b")
    rows = []
    for r in summary.itertuples():
        agree = vs.loc[r.engine] if r.engine in vs.index else None
        rows.append(
            [
                ENGINE_LABELS.get(r.engine, r.engine),
                fmt_seconds(r.load_median_s),
                fmt_seconds(r.compute_median_s),
                fmt_seconds(r.output_median_s),
                fmt_seconds(r.total_median_s),
                "–" if pd.isna(r.compute_vs_gds) else f"{r.compute_vs_gds:g}×",
                "–" if agree is None else fmt_float(agree["jaccard_top_k"], 2),
                "–" if agree is None else fmt_float(agree["spearman_rho"], 5),
            ]
        )
    k = int(agreement["top_k"].iloc[0])
    return ReportTable(
        "bench_pagerank",
        "PageRank engines on the same graph, median of 3 (F5.2, F5.3)",
        [
            "Engine",
            "Load",
            "Compute",
            "Write",
            "Total",
            "Compute vs GDS",
            f"Top-{k} J vs GDS",
            "ρ vs GDS",
        ],
        "lrrrrrrr",
        rows,
        ["benchmark_pagerank_summary.csv", "benchmark_pagerank_agreement.csv"],
        [
            "Unweighted PageRank on the COLLAB pairs, same damping and tolerance; GDS and "
            "GraphFrames on 4 cores, cuGraph on the laptop GPU (D-042, D-043).",
            _graph_note(summary),
        ],
    )


def bench_environment(env: pd.DataFrame, pagerank_runs: pd.DataFrame) -> ReportTable:
    rows = [[str(k), str(v)] for k, v in zip(env["key"], env["value"], strict=True)]
    gpu = pagerank_runs.dropna(subset=["gpu"])
    for col in ("gpu", "cugraph_version"):
        rows += [[f"{col} (F5.3)", str(v)] for v in sorted(gpu[col].astype(str).unique())]
    return ReportTable(
        "bench_environment",
        "Benchmark environment (F5.1, F5.3)",
        ["Key", "Value"],
        "ll",
        rows,
        ["benchmark_environment.csv", "benchmark_pagerank_runs.csv"],
        ["Spark rows describe the F5.1 scaling runs; the GPU rows the F5.3 cuGraph runs."],
    )
