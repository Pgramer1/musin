"""Data profiling report (F1.4, FR-03): evidence for the G1 open decisions.

    python -m musin.profiling --config config/pipeline.yaml [--sample 0.05]

Reads:  $MUSIN_DATA/<paths.raw>/*  (F1.3)
Writes: <reports_dir>/tables/profile_*.csv and <reports_dir>/figures/profile_*.png
        (full run), or $MUSIN_DATA/tmp/profiling_sample/ (with --sample, so a
        sample never overwrites report numbers).

--sample keeps recordings whose hashed id falls in the fraction (FR-12) and
profiles everything that hangs off them; table-level stats hash each table's
first column. Lookup-style tables (tags, genres, link types, releases) are
always read in full. Only aggregated frames are collected.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from musin.common.config import Config, load_config
from musin.common.sampling import sample_by_hash
from musin.common.spark import get_spark
from musin.profiling import figures
from musin.profiling.metrics import (
    credit_pairs,
    credit_sizes,
    featuring_credits,
    genre_tags,
    null_profile,
    recording_first_years,
    release_years,
    size_histogram,
)
from musin.schemas.tables import SCHEMAS

log = logging.getLogger("musin.profiling")

MEMBER_OF_BAND = "member of band"
CLASSICAL = "classical"


class Report:
    """Collects CSV tables and summary metrics for one run."""

    def __init__(self, out_dir: Path) -> None:
        self.tables = out_dir / "tables"
        self.figures = out_dir / "figures"
        self.tables.mkdir(parents=True, exist_ok=True)
        self.figures.mkdir(parents=True, exist_ok=True)
        self.summary: list[dict] = []

    def metric(self, section: str, name: str, value) -> None:
        self.summary.append({"section": section, "metric": name, "value": value})
        log.info("  %-14s %-52s %s", section, name, value)

    def write(self, name: str, rows: list[dict]) -> Path:
        path = self.tables / f"profile_{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            if rows:
                writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
        return path


def rows_of(df: DataFrame) -> list[dict]:
    """Collect an ALREADY AGGREGATED frame as dicts."""
    return [r.asDict() for r in df.collect()]


def share(part: int, whole: int) -> float | None:
    return round(part / whole, 6) if whole else None


def run(cfg: Config, sample: float | None) -> Report:
    repo_root = cfg.source.resolve().parent.parent
    out_dir = (
        cfg.data_root / "tmp" / "profiling_sample" if sample else repo_root / cfg.get("reports_dir")
    )
    rep = Report(out_dir)
    spark = get_spark("musin-profiling")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    max_size = cfg.get("filters.max_credit_size")
    snapshot_year = int(str(cfg.get("snapshot_date"))[:4])
    rep.metric("run", "snapshot", cfg.get("snapshot_date"))
    rep.metric("run", "sample_fraction", sample if sample else 1.0)

    # 1. Row counts and null rates --------------------------------------------------
    t0 = time.monotonic()
    nulls: list[dict] = []
    counts: list[dict] = []
    for table in sorted(SCHEMAS):
        df = raw(table)
        df = sample_by_hash(df, df.columns[0], sample)
        prof = null_profile(df, table)
        nulls += prof
        counts.append({"table": table, "rows": prof[0]["rows"], "columns": len(prof)})
    rep.write("row_counts", counts)
    rep.write("null_rates", nulls)
    log.info("row counts + null rates: %.0fs", time.monotonic() - t0)

    # Base frames reused below ------------------------------------------------------
    recording = (
        sample_by_hash(raw("recording"), "id", sample)
        .select(F.col("id").alias("recording"), "artist_credit")
        .cache()
    )
    n_rec = recording.count()
    acn = raw("artist_credit_name")
    acn_used = acn.join(recording.select("artist_credit").distinct(), "artist_credit").cache()
    sizes = credit_sizes(acn_used).cache()
    appearances = (
        recording.join(acn_used.select("artist_credit", "artist"), "artist_credit")
        .select("recording", "artist")
        .distinct()
        .cache()
    )
    n_app = appearances.count()
    rec_count = appearances.groupBy("artist").agg(F.count("*").alias("recording_count")).cache()
    n_artists = rec_count.count()
    rep.metric("base", "recordings", n_rec)
    rep.metric("base", "credited artists (on >= 1 recording)", n_artists)
    rep.metric("base", "artist-recording appearances", n_app)
    rep.metric("base", "artists in artist table (all)", counts_lookup(counts, "artist"))

    # 2. Credit sizes (OD3) ----------------------------------------------------------
    t0 = time.monotonic()
    rec_sizes = recording.join(sizes, "artist_credit", "left")
    no_credit = rec_sizes.filter(F.col("n_artists").isNull()).count()
    hist = rows_of(
        size_histogram(
            rec_sizes.na.drop(subset=["n_artists"]), cfg.get("profiling.credit_size_cap")
        )
    )
    total_pairs = sum(r["pair_instances"] for r in hist)
    cum_r = cum_p = 0
    for r in hist:
        cum_r += r["recordings"]
        cum_p += r["pair_instances"]
        r["recording_share"] = share(r["recordings"], n_rec)
        r["pair_share"] = share(r["pair_instances"], total_pairs)
        r["cumulative_recording_share"] = share(cum_r, n_rec)
        r["cumulative_pair_share"] = share(cum_p, total_pairs)
    rep.write("credit_sizes", hist)
    big = (
        rec_sizes.filter(F.col("n_artists") > max_size)
        .agg(
            F.count("*").alias("r"),
            F.sum((F.col("n_artists") * (F.col("n_artists") - 1) / 2).cast("long")).alias("p"),
        )
        .collect()[0]
    )
    rep.metric("credits", "recordings without credit names", no_credit)
    rep.metric("credits", "recordings with 1 credited artist", hist[0]["recordings"])
    rep.metric("credits", f"recordings with > {max_size} artists", big["r"])
    rep.metric(
        "credits", f"share of pair-instances from > {max_size}", share(big["p"] or 0, total_pairs)
    )
    rep.metric("credits", "total pair-instances (all sizes)", total_pairs)
    log.info("credit sizes: %.0fs", time.monotonic() - t0)

    # 3. Featuring and join phrases --------------------------------------------------
    t0 = time.monotonic()
    pattern = cfg.get("credits.featuring_pattern")
    feat = featuring_credits(acn_used, pattern)
    n_feat = recording.join(feat, "artist_credit").count()
    rep.metric("featuring", "recordings with a featured artist", n_feat)
    rep.metric("featuring", "share of recordings", share(n_feat, n_rec))
    multi = sum(r["recordings"] for r in hist if r["size_order"] > 1)
    rep.metric("featuring", "share of multi-artist recordings", share(n_feat, multi))
    phrases = rows_of(
        acn_used.groupBy("join_phrase")
        .agg(F.count("*").alias("credit_name_rows"))
        .orderBy(F.desc("credit_name_rows"), "join_phrase")
        .limit(cfg.get("profiling.top_k_join_phrases"))
        .withColumn("matches_featuring_pattern", F.col("join_phrase").rlike(pattern))
    )
    rep.write("join_phrases", phrases)
    log.info("featuring: %.0fs", time.monotonic() - t0)

    # 4. Release years (FR-07) -------------------------------------------------------
    t0 = time.monotonic()
    rel_years = release_years(raw("release_country"), raw("release_unknown_country")).cache()
    n_releases = raw("release").count()
    n_rel_dated = rel_years.count()
    track = (
        raw("track").select("recording", "medium").join(recording.select("recording"), "recording")
    )
    first = recording_first_years(track, raw("medium"), rel_years).cache()
    n_known = first.count()
    rep.metric("years", "releases", n_releases)
    rep.metric("years", "releases with a known year", n_rel_dated)
    rep.metric("years", "share of releases dated", share(n_rel_dated, n_releases))
    rep.metric("years", "recordings with a first release year", n_known)
    rep.metric("years", "share of recordings dated", share(n_known, n_rec))
    rep.metric("years", "first_year < 1900", first.filter(F.col("first_year") < 1900).count())
    rep.metric(
        "years",
        f"first_year > {snapshot_year}",
        first.filter(F.col("first_year") > snapshot_year).count(),
    )
    rep.metric("years", "min first_year", first.agg(F.min("first_year")).collect()[0][0])
    log.info("years: %.0fs", time.monotonic() - t0)

    # 5. Contribution relationships (OD5, OD7) ---------------------------------------
    t0 = time.monotonic()
    groups = {
        name: group
        for group in ("produced_for", "performed_for")
        for name in cfg.get(f"contributions.{group}")
    }
    lt = raw("link_type")
    link = raw("link").select(F.col("id").alias("link"), "link_type")
    group_col = F.create_map(*[x for kv in groups.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    def contribution_links(target: str) -> DataFrame:
        """(link, name, group) for allowlisted artist -> <target> link types."""
        types = lt.filter(
            (F.col("entity_type0") == "artist")
            & (F.col("entity_type1") == target)
            & F.col("name").isin(list(groups))
        ).select(F.col("id").alias("link_type"), "name")
        return link.join(types, "link_type").withColumn("group", group_col[F.col("name")])

    found = {
        r["name"]
        for r in lt.filter(
            (F.col("entity_type0") == "artist")
            & (F.col("entity_type1") == "recording")
            & F.col("name").isin(list(groups))
        )
        .select("name")
        .collect()
    }
    rep.metric(
        "contributions",
        "allowlisted names missing in link_type",
        sorted(set(groups) - found) or "none",
    )

    lar = (
        raw("l_artist_recording")
        .join(contribution_links("recording"), "link")
        .select(
            F.col("entity0").alias("artist"), F.col("entity1").alias("recording"), "name", "group"
        )
        .join(recording.select("recording"), "recording")
        .cache()
    )
    lare = (
        raw("l_artist_release")
        .join(contribution_links("release"), "link")
        .select(
            F.col("entity0").alias("artist"), F.col("entity1").alias("release"), "name", "group"
        )
        .cache()
    )
    contrib_rows = []
    for level, df, entity in (("recording", lar, "recording"), ("release", lare, "release")):
        for r in rows_of(
            df.groupBy("group", "name")
            .agg(
                F.count("*").alias("relationships"),
                F.countDistinct(entity).alias("distinct_targets"),
                F.countDistinct("artist").alias("distinct_artists"),
            )
            .orderBy("group", "name")
        ):
            contrib_rows.append({"level": level, **r})
    rep.write("contributions", contrib_rows)

    rec_grp = lar.select("recording", "group").distinct().cache()
    for group in ("produced_for", "performed_for"):
        n = rec_grp.filter(F.col("group") == group).count()
        rep.metric("contributions", f"recordings with {group} (recording level)", n)
        rep.metric("contributions", f"share with {group} (recording level)", share(n, n_rec))
    n_any = rec_grp.select("recording").distinct().count()
    rep.metric("contributions", "share with any contribution", share(n_any, n_rec))

    # OD7: what release-level credits would add
    rec_release = (
        track.join(raw("medium").select(F.col("id").alias("medium"), "release"), "medium")
        .select("recording", "release")
        .distinct()
    )
    via_release = (
        rec_release.join(lare.select("release", "group").distinct(), "release")
        .select("recording", "group")
        .distinct()
    )
    for group in ("produced_for", "performed_for"):
        vr = via_release.filter(F.col("group") == group).select("recording")
        rl = rec_grp.filter(F.col("group") == group).select("recording")
        n_vr = vr.count()
        n_new = vr.subtract(rl).count()
        rep.metric("od7", f"recordings with {group} via release level", n_vr)
        rep.metric("od7", "... of which NOT covered at recording level", n_new)
        rep.metric("od7", "... as share of all recordings", share(n_new, n_rec))

    # by decade (coverage bias over time)
    decade = (F.floor(F.col("first_year") / 10) * 10).cast("int")
    by_dec = (
        first.withColumn("decade", decade)
        .join(
            rec_grp.filter(F.col("group") == "produced_for")
            .select("recording")
            .withColumn("p", F.lit(1)),
            "recording",
            "left",
        )
        .join(
            rec_grp.filter(F.col("group") == "performed_for")
            .select("recording")
            .withColumn("q", F.lit(1)),
            "recording",
            "left",
        )
        .groupBy("decade")
        .agg(
            F.count("*").alias("recordings"),
            F.count("p").alias("with_produced_for"),
            F.count("q").alias("with_performed_for"),
        )
        .orderBy("decade")
    )
    dec_rows = rows_of(by_dec)
    for r in dec_rows:
        r["share_produced_for"] = share(r["with_produced_for"], r["recordings"])
        r["share_performed_for"] = share(r["with_performed_for"], r["recordings"])
    rep.write("years_by_decade", dec_rows)
    log.info("contributions: %.0fs", time.monotonic() - t0)

    # 6. Tags and genres (OD4, FR-08) ------------------------------------------------
    t0 = time.monotonic()
    gt = genre_tags(raw("tag"), raw("genre"), raw("genre_alias")).cache()
    at, rgt = raw("artist_tag"), raw("release_group_tag")
    rep.metric("tags", "official genres", raw("genre").count())
    rep.metric("tags", "tags matching an official genre", gt.count())
    rep.metric("tags", "artist_tag rows with count <= 0", at.filter(F.col("count") <= 0).count())
    rep.metric(
        "tags", "release_group_tag rows with count <= 0", rgt.filter(F.col("count") <= 0).count()
    )
    pos_votes = at.filter(F.col("count") > 0).agg(F.sum("count")).collect()[0][0] or 0
    genre_votes_artist = (
        at.filter(F.col("count") > 0).join(gt, "tag").agg(F.sum("count")).collect()[0][0] or 0
    )
    rep.metric(
        "tags",
        "share of positive artist-tag votes on genre tags",
        share(genre_votes_artist, pos_votes),
    )

    artist_genre = (
        at.filter(F.col("count") > 0).join(gt, "tag").select("artist", "genre_name", "count")
    )
    rg_artist = (
        raw("release_group")
        .select(F.col("id").alias("release_group"), "artist_credit")
        .join(acn.select("artist_credit", "artist"), "artist_credit")
    )
    rg_genre = (
        rgt.filter(F.col("count") > 0)
        .join(gt, "tag")
        .join(rg_artist, "release_group")
        .select("artist", "genre_name", "count")
    )
    a_art = artist_genre.select("artist").distinct().withColumn("art", F.lit(1))
    a_rg = rg_genre.select("artist").distinct().withColumn("rg", F.lit(1))
    cov = (
        rec_count.join(a_art, "artist", "left")
        .join(a_rg, "artist", "left")
        .agg(
            F.count("*").alias("artists"),
            F.count("art").alias("artist_level"),
            F.count("rg").alias("rg_level"),
            F.sum(F.when(F.col("art").isNotNull() | F.col("rg").isNotNull(), 1).otherwise(0)).alias(
                "either"
            ),
            F.sum(
                F.when(
                    F.col("art").isNotNull() | F.col("rg").isNotNull(), F.col("recording_count")
                ).otherwise(0)
            ).alias("app_either"),
            F.sum(F.when(F.col("art").isNotNull(), F.col("recording_count")).otherwise(0)).alias(
                "app_art"
            ),
        )
        .collect()[0]
    )
    rep.metric("tags", "credited artists with artist-level genre tag", cov["artist_level"])
    rep.metric("tags", "share (artist level)", share(cov["artist_level"], n_artists))
    rep.metric("tags", "credited artists with release-group genre tag", cov["rg_level"])
    rep.metric("tags", "share (release-group level)", share(cov["rg_level"], n_artists))
    rep.metric("tags", "share (either source)", share(cov["either"], n_artists))
    rep.metric("tags", "appearance-weighted share (artist level)", share(cov["app_art"], n_app))
    rep.metric("tags", "appearance-weighted share (either source)", share(cov["app_either"], n_app))

    top_genres = rows_of(
        artist_genre.groupBy("genre_name")
        .agg(
            F.sum("count").alias("artist_votes"), F.countDistinct("artist").alias("artists_tagged")
        )
        .join(
            rgt.filter(F.col("count") > 0)
            .join(gt, "tag")
            .groupBy("genre_name")
            .agg(
                F.sum("count").alias("rg_votes"),
                F.countDistinct("release_group").alias("release_groups_tagged"),
            ),
            "genre_name",
            "full",
        )
        .na.fill(0)
        .withColumn("total_votes", F.col("artist_votes") + F.col("rg_votes"))
        .orderBy(F.desc("total_votes"), "genre_name")
        .limit(cfg.get("profiling.top_k_genres"))
    )
    rep.write("top_genres", top_genres)
    log.info("tags: %.0fs", time.monotonic() - t0)

    # 7. Hubs, classical, thresholds, band members (OD1, OD2, FR-11) -----------------
    t0 = time.monotonic()
    pairs_by_credit = credit_pairs(acn_used, sizes, max_size)
    pair_recs = pairs_by_credit.join(recording, "artist_credit")
    pairs = pair_recs.groupBy("a", "b").agg(F.count("*").alias("weight")).cache()
    n_pairs = pairs.count()
    rep.metric("pairs", f"distinct credit pairs (credits <= {max_size})", n_pairs)
    partners = (
        pairs.select(F.col("a").alias("artist"), "b")
        .unionByName(pairs.select(F.col("b").alias("artist"), F.col("a").alias("b")))
        .groupBy("artist")
        .agg(F.count("*").alias("collaborators"))
    )

    top_tag = (
        artist_genre.withColumn(
            "rk",
            F.row_number().over(
                Window.partitionBy("artist").orderBy(F.desc("count"), "genre_name")
            ),
        )
        .filter("rk = 1")
        .select("artist", F.col("genre_name").alias("top_artist_genre_tag"))
    )
    artist_info = (
        raw("artist")
        .select(
            F.col("id").alias("artist"),
            F.col("gid").alias("mbid"),
            "name",
            F.col("type").alias("type_id"),
        )
        .join(
            raw("artist_type").select(
                F.col("id").alias("type_id"), F.col("name").alias("artist_type")
            ),
            "type_id",
            "left",
        )
        .drop("type_id")
    )
    k = cfg.get("profiling.top_k_hubs")
    hub_base = rec_count.join(partners, "artist", "left").na.fill(0, ["collaborators"])
    for name, order in (
        ("hubs_by_recordings", "recording_count"),
        ("hubs_by_collaborators", "collaborators"),
    ):
        top = hub_base.orderBy(F.desc(order), "artist").limit(k)
        rows = rows_of(
            top.join(artist_info, "artist", "left")
            .join(top_tag, "artist", "left")
            .orderBy(F.desc(order), "artist")
            .select(
                "artist",
                "mbid",
                "name",
                "artist_type",
                "recording_count",
                "collaborators",
                "top_artist_genre_tag",
            )
        )
        rep.write(name, rows)

    types = rows_of(
        rec_count.join(artist_info, "artist", "left")
        .groupBy("artist_type")
        .agg(F.count("*").alias("artists"), F.sum("recording_count").alias("appearances"))
        .orderBy(F.desc("artists"))
    )
    rep.write("artist_types", types)

    classical = (
        artist_genre.filter(F.col("genre_name") == CLASSICAL).select("artist").distinct().cache()
    )
    cl = (
        rec_count.join(classical, "artist").agg(F.count("*"), F.sum("recording_count")).collect()[0]
    )
    rep.metric("classical", "credited artists tagged 'classical'", cl[0])
    rep.metric("classical", "share of credited artists", share(cl[0], n_artists))
    rep.metric("classical", "share of appearances", share(cl[1] or 0, n_app))
    cl_ids = classical.withColumn("c", F.lit(1))
    cl_pairs = (
        pairs.join(
            cl_ids.withColumnRenamed("artist", "a").withColumnRenamed("c", "ca"), "a", "left"
        )
        .join(cl_ids.withColumnRenamed("artist", "b").withColumnRenamed("c", "cb"), "b", "left")
        .filter(F.col("ca").isNotNull() | F.col("cb").isNotNull())
        .agg(F.count("*"), F.sum("weight"))
        .collect()[0]
    )
    total_weight = pairs.agg(F.sum("weight")).collect()[0][0] or 0
    rep.metric(
        "classical", "share of distinct pairs touching classical", share(cl_pairs[0], n_pairs)
    )
    rep.metric(
        "classical",
        "share of pair weight touching classical",
        share(cl_pairs[1] or 0, total_weight),
    )

    grid = []
    for t in cfg.get("profiling.min_recordings_grid"):
        ok = rec_count.filter(F.col("recording_count") >= t).select("artist")
        e = pairs.join(ok.withColumnRenamed("artist", "a"), "a").join(
            ok.withColumnRenamed("artist", "b"), "b"
        )
        e = e.cache()
        n_e = e.count()
        n_nodes = (
            e.select(F.col("a").alias("x"))
            .unionByName(e.select(F.col("b").alias("x")))
            .distinct()
            .count()
        )
        grid.append(
            {
                "min_recordings": t,
                "artists_meeting_threshold": ok.count(),
                "nodes_with_an_edge": n_nodes,
                "credit_pair_edges": n_e,
            }
        )
        e.unpersist()
    rep.write("min_recordings_grid", grid)

    mob = lt.filter(
        (F.col("name") == MEMBER_OF_BAND)
        & (F.col("entity_type0") == "artist")
        & (F.col("entity_type1") == "artist")
    ).select(F.col("id").alias("link_type"))
    members = (
        raw("l_artist_artist")
        .join(link, "link")
        .join(mob, "link_type")
        .select("entity0", "entity1")
        .cache()
    )
    mrow = members.agg(
        F.count("*"), F.countDistinct("entity0"), F.countDistinct("entity1")
    ).collect()[0]
    rep.metric("band_members", "'member of band' relationships", mrow[0])
    rep.metric("band_members", "distinct members (entity0)", mrow[1])
    rep.metric("band_members", "distinct groups (entity1)", mrow[2])
    mpairs = members.select(
        F.least("entity0", "entity1").alias("a"), F.greatest("entity0", "entity1").alias("b")
    ).distinct()
    n_mp = pairs.join(mpairs, ["a", "b"]).count()
    rep.metric("band_members", "credit pairs that are group-member pairs", n_mp)
    rep.metric("band_members", "share of distinct credit pairs", share(n_mp, n_pairs))
    log.info("hubs/thresholds/members: %.0fs", time.monotonic() - t0)

    rep.write("summary", rep.summary)
    figures.render_all(rep.tables, rep.figures, rep.summary, max_size)
    for df in (
        recording,
        acn_used,
        sizes,
        appearances,
        rec_count,
        rel_years,
        first,
        lar,
        lare,
        rec_grp,
        gt,
        pairs,
        classical,
        members,
    ):
        df.unpersist()
    spark.stop()
    return rep


def counts_lookup(counts: list[dict], table: str) -> int:
    return next(c["rows"] for c in counts if c["table"] == table)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--sample", type=float, help="fraction of recordings, e.g. 0.05")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    rep = run(load_config(args.config), args.sample)
    log.info("done in %.0fs -> %s", time.monotonic() - start, rep.tables.parent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
