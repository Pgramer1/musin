"""Genre normalization: tags -> official genres -> parent genres (F2.5, FR-08).

    python -m musin.clean.genres --config config/pipeline.yaml

Reads:  $MUSIN_DATA/<paths.raw>/{tag,genre,genre_alias,artist_tag,release_group,
        release_group_tag,artist_credit_name}  (F1.3)
        $MUSIN_DATA/<paths.clean>/{artists,credits}  (F2.1, F2.2; full)
        <genres.mapping_file>  (official genre -> parent genre, D-024)
Writes: <clean>/artist_genres/         (artist_id, parent_genre, votes, share)
        <clean>/artist_primary_genre/  (artist_id, primary_genre, genre_votes, source, tied)
        <reports_dir>/tables/genre_{parent_summary,coverage,unmapped}.csv

A tag counts for a genre when its name equals the genre's name or an alias
(case-insensitive) and it has positive votes (D-015). Each artist's profile
comes from ONE source (D-025): its own artist tags if any map to a parent
genre, otherwise the tags of release groups on which it is a main credited
artist. `share` sums to 1 per artist; the primary genre is the highest share,
ties broken alphabetically (PRD §8.1). Artist-level, so there is no --sample.
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

from musin.clean.credits import MAIN, credit_roles
from musin.common.config import Config, ConfigError, load_config
from musin.common.spark import get_spark

log = logging.getLogger("musin.clean.genres")

MAPPING_COLUMNS = ["genre", "parent_genre", "basis"]
ARTIST, RELEASE_GROUP = "artist", "release_group"


def load_mapping(path: str | Path) -> dict[str, str]:
    """Read the genre mapping CSV: {official genre: parent genre or '' (unmapped)}.

    Raises ConfigError on a wrong header, a duplicate or empty genre name.
    """
    with Path(path).open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != MAPPING_COLUMNS:
            raise ConfigError(f"{path}: header must be {','.join(MAPPING_COLUMNS)}")
        mapping: dict[str, str] = {}
        for i, row in enumerate(reader, start=2):
            genre = (row["genre"] or "").strip()
            if not genre:
                raise ConfigError(f"{path}:{i}: empty genre")
            if genre in mapping:
                raise ConfigError(f"{path}:{i}: duplicate genre {genre!r}")
            mapping[genre] = (row["parent_genre"] or "").strip()
    return mapping


def check_mapping(mapping: dict[str, str], official: set[str]) -> None:
    """Every official genre must be listed, and every listed genre must exist.

    Raises ConfigError naming the differences, so a new snapshot or a typo
    cannot silently change which tags count.
    """
    missing = sorted(official - mapping.keys())
    unknown = sorted(mapping.keys() - official)
    if missing or unknown:
        raise ConfigError(
            f"genre mapping out of date: {len(missing)} official genres not listed "
            f"{missing[:10]}, {len(unknown)} listed genres not in the dump {unknown[:10]}"
        )


def genre_tags(tag: DataFrame, genre: DataFrame, genre_alias: DataFrame) -> DataFrame:
    """Tags that name an official genre, directly or through a genre alias.

    Matching is case-insensitive on the whole name.
    In: tag(id, name), genre(id, name), genre_alias(genre, name).
    Out: (tag, genre_name) with genre_name = the official genre's name.
    """
    names = genre.select(F.col("id").alias("genre_id"), F.lower("name").alias("lname")).unionByName(
        genre_alias.select(F.col("genre").alias("genre_id"), F.lower("name").alias("lname"))
    )
    official = genre.select(F.col("id").alias("genre_id"), F.col("name").alias("genre_name"))
    return (
        tag.select(F.col("id").alias("tag"), F.lower("name").alias("lname"))
        .join(names.distinct(), "lname")
        .join(official, "genre_id")
        .select("tag", "genre_name")
        .distinct()
    )


def parent_tags(genre_tag: DataFrame, mapping: DataFrame) -> DataFrame:
    """Tags that vote for a parent genre.

    In: genre_tags output, mapping(genre_name, parent_genre) with unmapped rows
    already removed. Out: distinct (tag, parent_genre); a tag matching two
    official genres of the same parent votes once for it.
    """
    return (
        genre_tag.join(F.broadcast(mapping), "genre_name").select("tag", "parent_genre").distinct()
    )


def tag_votes(tags: DataFrame, key: str, ptags: DataFrame) -> DataFrame:
    """Positive votes per (entity, parent genre).

    In: <entity>_tag(<key>, tag, count), parent_tags output.
    Out: (<key>, parent_genre, votes long); tags with count <= 0 are ignored.
    """
    return (
        tags.filter(F.col("count") > 0)
        .join(ptags, "tag")
        .groupBy(key, "parent_genre")
        .agg(F.sum("count").cast("long").alias("votes"))
    )


def main_artists_of_release_groups(
    release_group: DataFrame, acn: DataFrame, pattern: str
) -> DataFrame:
    """(release_group, artist) for every MAIN artist on a release group's credit.

    Same featured rule as F2.2 (PRD §8.2), so featured guests don't inherit the
    album's genre. In: release_group(id, artist_credit), artist_credit_name.
    Out: distinct (release_group, artist).
    """
    rg = release_group.select(F.col("id").alias("release_group"), "artist_credit")
    roles = credit_roles(acn.join(rg.select("artist_credit").distinct(), "artist_credit"), pattern)
    mains = roles.filter(F.col("role") == MAIN).select("artist_credit", "artist")
    return rg.join(mains, "artist_credit").select("release_group", "artist").distinct()


def choose_source(artist_votes: DataFrame, rg_votes: DataFrame) -> DataFrame:
    """One genre-vote source per artist: artist tags if any, else release-group tags.

    In: (artist, parent_genre, votes) from each source.
    Out: (artist, parent_genre, votes, source) with source 'artist' or 'release_group'.
    """
    have_own = artist_votes.select("artist").distinct()
    return artist_votes.withColumn("source", F.lit(ARTIST)).unionByName(
        rg_votes.join(have_own, "artist", "left_anti").withColumn("source", F.lit(RELEASE_GROUP))
    )


def profiles(votes: DataFrame) -> DataFrame:
    """Genre shares per artist.

    In: (artist_id, parent_genre, votes, source). Out: (artist_id, parent_genre,
    votes, share, source) with share = votes / artist total (sums to 1).
    """
    total = Window.partitionBy("artist_id")
    return votes.withColumn("share", F.col("votes") / F.sum("votes").over(total))


def primary_genres(profile: DataFrame) -> DataFrame:
    """Primary genre per artist: highest share, ties broken alphabetically (PRD §8.1).

    In: profiles output. Out: (artist_id, primary_genre, genre_votes, source, tied)
    where genre_votes is the artist's total votes and tied is true if another
    parent genre had the same top share.
    """
    by_share = Window.partitionBy("artist_id").orderBy(F.desc("share"), "parent_genre")
    per_artist = Window.partitionBy("artist_id")
    return (
        profile.withColumn("rank", F.row_number().over(by_share))
        .withColumn("top_share", F.max("share").over(per_artist))
        .withColumn(
            "n_top", F.sum((F.col("share") == F.col("top_share")).cast("int")).over(per_artist)
        )
        .withColumn("genre_votes", F.sum("votes").over(per_artist))
        .filter(F.col("rank") == 1)
        .select(
            "artist_id",
            F.col("parent_genre").alias("primary_genre"),
            F.col("genre_votes").cast("long"),
            "source",
            (F.col("n_top") > 1).alias("tied"),
        )
    )


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(cfg: Config) -> dict:
    """Run the stage; return the logged counts."""
    repo_root = cfg.source.resolve().parent.parent
    mapping = load_mapping(repo_root / cfg.get("genres.mapping_file"))
    pattern = cfg.get("credits.featuring_pattern")
    spark = get_spark("musin-clean-genres")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    clean = cfg.data_path("clean")
    genre = raw("genre")
    check_mapping(mapping, {r["name"] for r in genre.select("name").collect()})  # 2,212 rows
    mapped = spark.createDataFrame(
        [(g, p) for g, p in mapping.items() if p], "genre_name string, parent_genre string"
    )
    gtags = genre_tags(raw("tag"), genre, raw("genre_alias")).cache()
    ptags = parent_tags(gtags, mapped).cache()

    artist_votes = tag_votes(raw("artist_tag"), "artist", ptags)
    rg_main = main_artists_of_release_groups(
        raw("release_group"), raw("artist_credit_name"), pattern
    )
    rg_votes = (
        tag_votes(raw("release_group_tag"), "release_group", ptags)
        .join(rg_main, "release_group")
        .groupBy("artist", "parent_genre")
        .agg(F.sum("votes").cast("long").alias("votes"))
    )
    artists = spark.read.parquet(str(clean / "artists")).select("artist_id")
    votes = (
        choose_source(artist_votes, rg_votes)
        .withColumn("artist_id", F.col("artist").cast("long"))
        .join(artists, "artist_id")  # drops special artists (FR-04)
        .select("artist_id", "parent_genre", "votes", "source")
    )
    profile = profiles(votes)
    profile.select("artist_id", "parent_genre", "votes", "share").write.mode("overwrite").parquet(
        str(clean / "artist_genres")
    )
    primary_genres(profile).write.mode("overwrite").parquet(str(clean / "artist_primary_genre"))
    genres_out = spark.read.parquet(str(clean / "artist_genres"))
    primary = spark.read.parquet(str(clean / "artist_primary_genre")).cache()

    # --- counts and report tables (aggregated frames only) ----------------------
    credited = (
        spark.read.parquet(str(clean / "credits"))
        .groupBy("artist_id")
        .agg(F.count("*").alias("appearances"))
        .cache()
    )
    cov = (
        credited.join(primary, "artist_id", "left")
        .agg(
            F.count("*").alias("credited"),
            F.sum("appearances").alias("appearances"),
            F.count("primary_genre").alias("credited_with_genre"),
            F.sum(F.when(F.col("primary_genre").isNotNull(), F.col("appearances"))).alias(
                "appearances_with_genre"
            ),
            F.sum((F.col("source") == ARTIST).cast("long")).alias("credited_source_artist"),
            F.sum((F.col("source") == RELEASE_GROUP).cast("long")).alias("credited_source_rg"),
            F.sum(F.col("tied").cast("long")).alias("credited_tied"),
        )
        .collect()[0]
    )
    prim_stats = primary.agg(
        F.count("*").alias("artists_with_genre"),
        F.sum((F.col("source") == ARTIST).cast("long")).alias("source_artist"),
        F.sum((F.col("source") == RELEASE_GROUP).cast("long")).alias("source_rg"),
        F.sum(F.col("tied").cast("long")).alias("tied"),
    ).collect()[0]
    share_check = (
        genres_out.groupBy("artist_id")
        .agg(F.sum("share").alias("s"))
        .filter(F.abs(F.col("s") - 1) > 1e-9)
        .count()
    )
    gvotes = (
        raw("artist_tag")
        .filter("count > 0")
        .join(gtags, "tag")
        .groupBy("genre_name")
        .agg(F.sum("count").alias("artist_votes"))
        .join(
            raw("release_group_tag")
            .filter("count > 0")
            .join(gtags, "tag")
            .groupBy("genre_name")
            .agg(F.sum("count").alias("rg_votes")),
            "genre_name",
            "full",
        )
        .na.fill(0)
        .collect()  # one row per official genre with votes (< 2,212)
    )
    total_votes = sum(r["artist_votes"] + r["rg_votes"] for r in gvotes)
    mapped_votes = sum(
        r["artist_votes"] + r["rg_votes"] for r in gvotes if mapping[r["genre_name"]]
    )

    counts = {
        "official_genres": len(mapping),
        "official_genres_mapped": sum(1 for p in mapping.values() if p),
        "parent_genres": len({p for p in mapping.values() if p}),
        "genre_tags": gtags.count(),
        "parent_tags": ptags.count(),
        "share_of_genre_votes_mapped": round(mapped_votes / total_votes, 6),
        "artists_with_genre": prim_stats["artists_with_genre"],
        "artists_source_artist_tags": prim_stats["source_artist"],
        "artists_source_release_group_tags": prim_stats["source_rg"],
        "artists_primary_tied": prim_stats["tied"],
        "artist_genre_rows": genres_out.count(),
        "credited_artists": cov["credited"],
        "credited_with_genre": cov["credited_with_genre"],
        "credited_share_with_genre": round(cov["credited_with_genre"] / cov["credited"], 6),
        "appearance_share_with_genre": round(cov["appearances_with_genre"] / cov["appearances"], 6),
        "credited_source_artist_tags": cov["credited_source_artist"],
        "credited_source_release_group_tags": cov["credited_source_rg"],
        "credited_primary_tied": cov["credited_tied"],
    }
    for key, value in counts.items():
        log.info("  %-38s %s", key, value)

    tables = repo_root / cfg.get("reports_dir") / "tables"
    by_parent = {
        r["primary_genre"]: r
        for r in credited.join(primary, "artist_id")
        .groupBy("primary_genre")
        .agg(F.count("*").alias("credited_artists"), F.sum("appearances").alias("appearances"))
        .collect()
    }
    all_primary = {
        r["primary_genre"]: r["count"] for r in primary.groupBy("primary_genre").count().collect()
    }
    vote_by_parent: dict[str, int] = {}
    for r in gvotes:
        p = mapping[r["genre_name"]]
        if p:
            vote_by_parent[p] = vote_by_parent.get(p, 0) + r["artist_votes"] + r["rg_votes"]
    parents = sorted({p for p in mapping.values() if p})
    write_table(
        tables / "genre_parent_summary.csv",
        [
            {
                "parent_genre": p,
                "official_genres_mapped": sum(1 for v in mapping.values() if v == p),
                "tag_votes": vote_by_parent.get(p, 0),
                "artists_primary": all_primary.get(p, 0),
                "credited_artists_primary": by_parent[p]["credited_artists"]
                if p in by_parent
                else 0,
                "credited_share": round(
                    (by_parent[p]["credited_artists"] if p in by_parent else 0)
                    / cov["credited_with_genre"],
                    6,
                ),
                "appearances_primary": by_parent[p]["appearances"] if p in by_parent else 0,
            }
            for p in parents
        ],
    )
    write_table(
        tables / "genre_coverage.csv", [{"metric": k, "value": v} for k, v in counts.items()]
    )
    unmapped = sorted(
        (r for r in gvotes if not mapping[r["genre_name"]]),
        key=lambda r: (-(r["artist_votes"] + r["rg_votes"]), r["genre_name"]),
    )
    write_table(
        tables / "genre_unmapped.csv",
        [
            {
                "genre": r["genre_name"],
                "artist_votes": r["artist_votes"],
                "rg_votes": r["rg_votes"],
                "share_of_genre_votes": round((r["artist_votes"] + r["rg_votes"]) / total_votes, 6),
            }
            for r in unmapped
        ],
    )

    problems = []
    if share_check:
        problems.append(f"{share_check} artists whose shares do not sum to 1")
    if primary.groupBy("artist_id").count().filter("count > 1").count():
        problems.append("more than one primary genre per artist")
    if set(all_primary) - set(parents):
        problems.append("primary genre outside the parent list")
    for df in (gtags, ptags, primary, credited):
        df.unpersist()
    spark.stop()
    if problems:
        raise RuntimeError("quality checks failed: " + "; ".join(problems))
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    run(load_config(args.config))
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
