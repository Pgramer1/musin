"""Report tables derived from other report tables (F6.1, FR-24).

Pure pandas on already aggregated CSVs from reports/tables: nothing here
reads the graph, Spark or Neo4j.

- sensitivity_top_lists / sensitivity_agreement: the main graph against each
  F4.7 variant (D-035). F4.7 compared them by hand; these make the comparison
  a generated table, so the report can quote it (RULES §9).
- instance_fates: what happened to every edge instance in F3.1, by edge type.
- pipeline_funnel: from dump rows to the loaded graph, one row per step, each
  copied from a named metric of a named table.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from musin.graph.build import EDGE_TYPES

# F3.1 drop reasons in graph_build_summary.csv (instances_<TYPE>_<fate>), kept first.
FATES = (
    "kept",
    "self_loop",
    "excluded_classical",
    "band_member",
    "same_recording_pair",
    "excluded_artist_type",
)
MEASURES = ("degreeWeighted", "pagerank", "betweenness")


def admitted_predicate(main_filters: dict, variant_filters: dict) -> Callable[[pd.Series], bool]:
    """Whether an artist row (type, primary_genre) is one the variant lets in.

    In: the `filters` section of the main config and of the variant config.
    A Character artist is admitted when the variant drops `Character` from
    exclude_artist_types (D-021); a classical artist when the variant sets
    include_classical and the main graph doesn't (D-039). Other variant keys
    admit no particular kind of artist, so the predicate is then always false.
    """
    newly_typed = set(main_filters["exclude_artist_types"]) - set(
        variant_filters["exclude_artist_types"]
    )
    classical = bool(variant_filters["include_classical"]) and not bool(
        main_filters["include_classical"]
    )

    def admitted(row: pd.Series) -> bool:
        if row.get("type") in newly_typed:
            return True
        return classical and row.get("primary_genre") == "classical"

    return admitted


def _compare_lists(
    main: pd.DataFrame, variant: pd.DataFrame, rank_col: str, admitted: Callable
) -> dict:
    """Overlap of two top lists (by MBID) and the variant's entrants."""
    a, b = set(main["mbid"]), set(variant["mbid"])
    entrants = variant[~variant["mbid"].isin(a)].sort_values(rank_col, kind="mergesort")
    let_in = entrants[entrants.apply(admitted, axis=1)] if len(entrants) else entrants
    return {
        "main_artists": len(a),
        "variant_artists": len(b),
        "common": len(a & b),
        "jaccard": len(a & b) / len(a | b) if a | b else float("nan"),
        "entrants": len(entrants),
        "admitted_entrants": len(let_in),
        "admitted_names": "; ".join(
            f"{r['name']} #{int(r[rank_col])}" for _, r in let_in.iterrows()
        ),
        "other_entrant_names": "; ".join(
            f"{r['name']} #{int(r[rank_col])}"
            for _, r in entrants[~entrants["mbid"].isin(set(let_in["mbid"]))].iterrows()
        ),
    }


def sensitivity_top_lists(
    variant: str,
    main_top: pd.DataFrame,
    var_top: pd.DataFrame,
    main_bridges: pd.DataFrame,
    var_bridges: pd.DataFrame,
    k_centrality: int,
    k_bridges: int,
    admitted: Callable[[pd.Series], bool],
) -> pd.DataFrame:
    """Top-k overlap between the main graph and one variant, per list.

    In: centrality_top.csv (projection, measure, rank, mbid, name, type,
    primary_genre, graph, edges_fingerprint) and bridges_top.csv (bridge_rank,
    mbid, name, type, primary_genre, ...) of both graphs. Out: one row per
    centrality (projection, measure) and one for the bridge list, with variant,
    list, projection, measure, k, main_artists, variant_artists, common,
    jaccard, entrants, admitted_entrants, admitted_names ("name #rank"),
    other_entrant_names, and both graphs' names and fingerprints.
    """
    rows = []
    for projection in sorted(main_top["projection"].unique()):
        for measure in MEASURES:

            def pick(frame: pd.DataFrame, p=projection, m=measure) -> pd.DataFrame:
                sel = (frame["projection"] == p) & (frame["measure"] == m)
                return frame[sel & (frame["rank"] <= k_centrality)]

            a, b = pick(main_top), pick(var_top)
            if a.empty or b.empty:
                raise ValueError(f"{variant}: no {projection}/{measure} top list")
            rows.append(
                {"list": "centrality", "projection": projection, "measure": measure}
                | {"k": k_centrality}
                | _compare_lists(a, b, "rank", admitted)
            )
    a = main_bridges[main_bridges["bridge_rank"] <= k_bridges]
    b = var_bridges[var_bridges["bridge_rank"] <= k_bridges]
    rows.append(
        {"list": "bridges", "projection": "collab_undirected", "measure": "bridgeScore"}
        | {"k": k_bridges}
        | _compare_lists(a, b, "bridge_rank", admitted)
    )
    out = pd.DataFrame(rows)
    out.insert(0, "variant", variant)
    return out.assign(
        graph_main=main_top["graph"].iloc[0],
        edges_fingerprint_main=main_top["edges_fingerprint"].iloc[0],
        graph_variant=var_top["graph"].iloc[0],
        edges_fingerprint_variant=var_top["edges_fingerprint"].iloc[0],
    )


def sensitivity_agreement(
    variant: str, main_agree: pd.DataFrame, var_agree: pd.DataFrame
) -> pd.DataFrame:
    """Centrality agreement (F4.1) of the main graph next to one variant's.

    In: centrality_agreement.csv of both graphs (comparison, measure_a,
    measure_b, n_artists, spearman_rho, top_k, jaccard_top_k, graph,
    edges_fingerprint). Out: one row per comparison with variant, comparison,
    measure_a, measure_b, top_k, n_artists_{main,variant},
    spearman_rho_{main,variant}, rho_delta, jaccard_top_k_{main,variant},
    jaccard_delta, and both graphs' names and fingerprints.
    """
    keys = ["comparison", "measure_a", "measure_b", "top_k"]
    cols = [*keys, "n_artists", "spearman_rho", "jaccard_top_k", "graph", "edges_fingerprint"]
    merged = main_agree[cols].merge(
        var_agree[cols], on=keys, how="outer", suffixes=("_main", "_variant"), validate="1:1"
    )
    if merged[["spearman_rho_main", "spearman_rho_variant"]].isna().any().any():
        raise ValueError(f"{variant}: agreement rows differ from the main graph's")
    merged["rho_delta"] = merged["spearman_rho_variant"] - merged["spearman_rho_main"]
    merged["jaccard_delta"] = merged["jaccard_top_k_variant"] - merged["jaccard_top_k_main"]
    merged.insert(0, "variant", variant)
    order = [
        "variant",
        *keys,
        "n_artists_main",
        "n_artists_variant",
        "spearman_rho_main",
        "spearman_rho_variant",
        "rho_delta",
        "jaccard_top_k_main",
        "jaccard_top_k_variant",
        "jaccard_delta",
        "graph_main",
        "edges_fingerprint_main",
        "graph_variant",
        "edges_fingerprint_variant",
    ]
    return merged[order]


def _metrics(summary: pd.DataFrame) -> dict[str, float]:
    """graph_build_summary.csv (metric, value) as a dict; duplicates are an error."""
    if summary["metric"].duplicated().any():
        raise ValueError("graph_build_summary.csv has duplicate metrics")
    return dict(zip(summary["metric"], summary["value"].astype(float), strict=True))


def _one_metric(metrics: dict[str, float], prefix: str) -> tuple[str, float]:
    """The single metric whose name starts with `prefix` (names embed the credit-size cap)."""
    found = [(k, v) for k, v in metrics.items() if k.startswith(prefix)]
    if len(found) != 1:
        raise ValueError(f"expected one graph_build_summary metric {prefix}*, got {found}")
    return found[0]


def instance_fates(summary: pd.DataFrame, graph: str, fingerprint: str) -> pd.DataFrame:
    """Edge instances per (edge type, fate) from F3.1's graph_build_summary.csv.

    In: metric, value with instances_<TYPE>_<fate> rows. Out: edge_type, fate,
    instances, share_of_type (of all instances of that type), graph,
    edges_fingerprint; edge types in PRD §8.2 order, fates in FATES order.
    """
    metrics = _metrics(summary)
    rows = []
    for edge_type in EDGE_TYPES:
        counts = {f: metrics.get(f"instances_{edge_type}_{f}") for f in FATES}
        missing = [f for f, v in counts.items() if v is None]
        if missing:
            raise ValueError(f"graph_build_summary.csv lacks {edge_type} fates {missing}")
        total = sum(counts.values())
        rows += [
            {"edge_type": edge_type, "fate": f, "instances": int(v), "share_of_type": v / total}
            for f, v in counts.items()
        ]
    return pd.DataFrame(rows).assign(graph=graph, edges_fingerprint=fingerprint)


def pipeline_funnel(
    row_counts: pd.DataFrame,
    profile: pd.DataFrame,
    summary: pd.DataFrame,
    projections: pd.DataFrame,
    graph: str,
    fingerprint: str,
) -> pd.DataFrame:
    """From dump rows to the loaded graph, each value copied from a named table metric.

    In: profile_row_counts.csv (table, rows), profile_summary.csv (section,
    metric, value), graph_build_summary.csv (metric, value) and
    gds_projections.csv (projection, pairs, total_weight). Out: step, layer,
    quantity, value, source_table, source_metric, graph, edges_fingerprint.
    """
    rows_of = dict(zip(row_counts["table"], row_counts["rows"], strict=True))
    prof = {
        (s, m): v for s, m, v in profile[["section", "metric", "value"]].itertuples(index=False)
    }
    m = _metrics(summary)
    collab = projections[projections["projection"] == "collab_undirected"]
    if len(collab) != 1:
        raise ValueError("gds_projections.csv needs exactly one collab_undirected row")
    over_key, over = _one_metric(m, "recordings_over_")
    skipped_key, skipped = _one_metric(m, "pair_instances_skipped_over_")
    kept = sum(m[f"instances_{t}_kept"] for t in EDGE_TYPES)
    dropped = sum(m[f"instances_{t}_{f}"] for t in EDGE_TYPES for f in FATES if f != "kept")
    steps = [
        ("dump", "recordings", rows_of["recording"], "profile_row_counts.csv", "recording"),
        ("dump", "tracks", rows_of["track"], "profile_row_counts.csv", "track"),
        ("dump", "artists", rows_of["artist"], "profile_row_counts.csv", "artist"),
        (
            "dump",
            "artist credit names (billing rows)",
            rows_of["artist_credit_name"],
            "profile_row_counts.csv",
            "artist_credit_name",
        ),
        (
            "dump",
            "artist–recording relationships",
            rows_of["l_artist_recording"],
            "profile_row_counts.csv",
            "l_artist_recording",
        ),
        (
            "profile",
            "credited artists (on at least one recording)",
            prof[("base", "credited artists (on >= 1 recording)")],
            "profile_summary.csv",
            "base / credited artists (on >= 1 recording)",
        ),
        (
            "graph build",
            "recordings entering the edge build",
            m["recordings_in"],
            "graph_build_summary.csv",
            "recordings_in",
        ),
        (
            "graph build",
            "recordings over the credit-size cap (no edges)",
            over,
            "graph_build_summary.csv",
            over_key,
        ),
        (
            "graph build",
            "pair instances skipped by the credit-size cap",
            skipped,
            "graph_build_summary.csv",
            skipped_key,
        ),
        (
            "graph build",
            "artists excluded by type (Character)",
            m["excluded_artists_by_type"],
            "graph_build_summary.csv",
            "excluded_artists_by_type",
        ),
        (
            "graph build",
            "artists excluded as classical",
            m["excluded_artists_classical"],
            "graph_build_summary.csv",
            "excluded_artists_classical",
        ),
        (
            "graph build",
            "edge instances dropped (all reasons)",
            dropped,
            "graph_build_summary.csv",
            "instances_<type>_<reason>, summed",
        ),
        (
            "graph build",
            "edge instances kept",
            kept,
            "graph_build_summary.csv",
            "instances_<type>_kept, summed",
        ),
        (
            "graph",
            "edges (artist pair × type)",
            m["edges_before_node_rule"] - m["edges_removed_by_min_recordings"],
            "graph_build_summary.csv",
            "edges_before_node_rule − edges_removed_by_min_recordings",
        ),
        ("graph", "artists (nodes)", m["nodes"], "graph_build_summary.csv", "nodes"),
        (
            "Neo4j",
            "COLLAB artist pairs",
            collab["pairs"].iloc[0],
            "gds_projections.csv",
            "collab_undirected / pairs",
        ),
        (
            "Neo4j",
            "COLLAB weight (shared recordings)",
            collab["total_weight"].iloc[0],
            "gds_projections.csv",
            "collab_undirected / total_weight",
        ),
    ]
    out = pd.DataFrame(
        steps, columns=["layer", "quantity", "value", "source_table", "source_metric"]
    )
    out["value"] = out["value"].astype(float).round().astype("int64")
    out.insert(0, "step", range(1, len(out) + 1))
    return out.assign(graph=graph, edges_fingerprint=fingerprint)
