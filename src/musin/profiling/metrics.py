"""Profiling building blocks (F1.4). Pure DataFrame -> DataFrame transformations.

Every function documents its input and output columns (RULES.md §5). Inputs
are raw/ tables (MusicBrainz column names); nothing here filters the data.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from musin.clean import years


def credit_sizes(acn: DataFrame) -> DataFrame:
    """Distinct credited artists per artist credit.

    In: artist_credit_name(artist_credit, artist). Out: (artist_credit, n_artists).
    """
    return acn.groupBy("artist_credit").agg(F.countDistinct("artist").alias("n_artists"))


def credit_pairs(acn: DataFrame, sizes: DataFrame, max_size: int) -> DataFrame:
    """Unordered artist pairs within each credit of at most `max_size` artists.

    In: artist_credit_name(artist_credit, artist), credit_sizes output.
    Out: (artist_credit, a, b) with a < b, one row per pair per credit.
    """
    small = acn.join(sizes.filter(F.col("n_artists") <= max_size), "artist_credit")
    left = small.select("artist_credit", F.col("artist").alias("a")).distinct()
    right = small.select("artist_credit", F.col("artist").alias("b")).distinct()
    return left.join(right, "artist_credit").filter(F.col("a") < F.col("b"))


def featuring_credits(acn: DataFrame, pattern: str) -> DataFrame:
    """Credits with at least one featured artist (PRD §8.2).

    An artist is featured if it comes after a join phrase matching `pattern`,
    so a credit has one when a non-final position's join phrase matches.
    In: artist_credit_name(artist_credit, position, join_phrase). Out: (artist_credit).
    """
    last = Window.partitionBy("artist_credit")
    return (
        acn.withColumn("last_position", F.max("position").over(last))
        .filter((F.col("position") < F.col("last_position")) & F.col("join_phrase").rlike(pattern))
        .select("artist_credit")
        .distinct()
    )


def release_years(release_country: DataFrame, release_unknown_country: DataFrame) -> DataFrame:
    """Earliest known year per release, without plausibility bounds.

    The profile reports raw data, so no year is ignored here; the clean stage
    applies D-023's bounds. In: release_country(release, date_year),
    release_unknown_country(release, date_year). Out: (release, year).
    """
    return years.release_years(release_country, release_unknown_country, None, None)


recording_first_years = years.recording_first_years


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


def size_histogram(sizes_per_recording: DataFrame, cap: int) -> DataFrame:
    """Recordings and pair-instances per credit size, sizes above `cap` bucketed.

    In: (recording, n_artists). Out: (size_order, size_bucket, recordings, pair_instances)
    where pair_instances = sum of n*(n-1)/2 over the bucket's recordings.
    """
    n = F.col("n_artists")
    return (
        sizes_per_recording.withColumn("size_order", F.least(n, F.lit(cap + 1)))
        .withColumn(
            "size_bucket",
            F.when(n > cap, F.lit(f">{cap}")).otherwise(n.cast(StringType())),
        )
        .groupBy("size_order", "size_bucket")
        .agg(
            F.count("*").alias("recordings"),
            F.sum((n * (n - 1) / 2).cast("long")).alias("pair_instances"),
        )
        .orderBy("size_order")
    )


def null_profile(df: DataFrame, table: str) -> list[dict]:
    """Nulls (and empty strings for text) per column, as plain dicts.

    In: any frame. Out: one dict per column with table, column, type, rows,
    nulls, null_rate, empty (string columns only, else None). Collects one
    aggregated row only.
    """
    aggs = [F.count("*").alias("__rows")]
    for f in df.schema.fields:
        aggs.append(F.sum(F.col(f.name).isNull().cast("long")).alias(f"n__{f.name}"))
        if isinstance(f.dataType, StringType):
            aggs.append(F.sum((F.col(f.name) == "").cast("long")).alias(f"e__{f.name}"))
    row = df.agg(*aggs).collect()[0].asDict()
    rows = row["__rows"]
    out = []
    for f in df.schema.fields:
        nulls = row[f"n__{f.name}"] or 0
        out.append(
            {
                "table": table,
                "column": f.name,
                "type": f.dataType.simpleString(),
                "rows": rows,
                "nulls": nulls,
                "null_rate": round(nulls / rows, 6) if rows else None,
                "empty": row.get(f"e__{f.name}"),
            }
        )
    return out
