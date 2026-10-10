"""Tests for musin.profiling.metrics and musin.common.sampling on tiny frames."""

import pytest

from musin.common.sampling import sample_by_hash
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

FEAT = r"(?i)\b(feat|ft|featuring)\b"


@pytest.fixture
def acn(spark):
    # (artist_credit, position, artist, join_phrase)
    rows = [
        (1, 0, 10, " feat. "),
        (1, 1, 11, ""),
        (2, 0, 10, " & "),
        (2, 1, 12, " & "),
        (2, 2, 13, ""),
        (3, 0, 14, ""),
        (4, 0, 15, " feat."),  # featuring phrase on the LAST position: no featured artist
        (5, 0, 16, " / "),
        (5, 1, 16, ""),  # same artist twice in one credit
    ]
    return spark.createDataFrame(rows, ["artist_credit", "position", "artist", "join_phrase"])


def as_set(df, *cols):
    return {tuple(r[c] for c in cols) for r in df.collect()}


def test_credit_sizes_counts_distinct_artists(acn):
    assert as_set(credit_sizes(acn), "artist_credit", "n_artists") == {
        (1, 2),
        (2, 3),
        (3, 1),
        (4, 1),
        (5, 1),
    }


def test_credit_pairs_respects_max_size(acn):
    sizes = credit_sizes(acn)
    assert as_set(credit_pairs(acn, sizes, 2), "artist_credit", "a", "b") == {(1, 10, 11)}
    assert as_set(credit_pairs(acn, sizes, 3), "artist_credit", "a", "b") == {
        (1, 10, 11),
        (2, 10, 12),
        (2, 10, 13),
        (2, 12, 13),
    }


def test_featuring_credits(acn):
    assert as_set(featuring_credits(acn, FEAT), "artist_credit") == {(1,)}


def test_featuring_pattern_word_boundaries(spark):
    rows = [
        (1, 0, 1, " Ft. "),
        (1, 1, 2, ""),
        (2, 0, 3, " Featuring "),
        (2, 1, 4, ""),
        (3, 0, 5, " left "),
        (3, 1, 6, ""),
    ]  # "left" contains "ft" but not as a word
    df = spark.createDataFrame(rows, ["artist_credit", "position", "artist", "join_phrase"])
    assert as_set(featuring_credits(df, FEAT), "artist_credit") == {(1,), (2,)}


def test_release_and_recording_years(spark):
    rc = spark.createDataFrame(
        [(100, 1999), (100, 1997), (101, None)], "release int, date_year short"
    )
    ruc = spark.createDataFrame([(100, 2001), (102, 1985)], "release int, date_year short")
    years = release_years(rc, ruc)
    assert as_set(years, "release", "year") == {(100, 1997), (102, 1985)}

    medium = spark.createDataFrame([(1, 100), (2, 102), (3, 101)], ["id", "release"])
    track = spark.createDataFrame([(7, 1), (7, 2), (8, 3)], ["recording", "medium"])
    assert as_set(recording_first_years(track, medium, years), "recording", "first_year") == {
        (7, 1985)
    }


def test_genre_tags_match_names_and_aliases(spark):
    tag = spark.createDataFrame(
        [(1, "Hip-Hop"), (2, "rock"), (3, "seen live"), (4, "HIP HOP")], ["id", "name"]
    )
    genre = spark.createDataFrame([(50, "hip hop"), (51, "rock")], ["id", "name"])
    alias = spark.createDataFrame([(50, "hip-hop")], ["genre", "name"])
    assert as_set(genre_tags(tag, genre, alias), "tag", "genre_name") == {
        (1, "hip hop"),
        (2, "rock"),
        (4, "hip hop"),
    }


def test_size_histogram_buckets_and_pairs(spark):
    df = spark.createDataFrame(
        [(1, 1), (2, 2), (3, 2), (4, 3), (5, 30)], ["recording", "n_artists"]
    )
    rows = [r.asDict() for r in size_histogram(df, 2).collect()]
    assert rows == [
        {"size_order": 1, "size_bucket": "1", "recordings": 1, "pair_instances": 0},
        {"size_order": 2, "size_bucket": "2", "recordings": 2, "pair_instances": 2},
        {"size_order": 3, "size_bucket": ">2", "recordings": 2, "pair_instances": 3 + 435},
    ]


def test_null_profile(spark):
    df = spark.createDataFrame([(1, "a"), (None, ""), (3, None)], "x int, s string")
    prof = {p["column"]: p for p in null_profile(df, "t")}
    assert prof["x"]["nulls"] == 1 and prof["x"]["empty"] is None
    assert prof["s"]["nulls"] == 1 and prof["s"]["empty"] == 1
    assert prof["s"]["rows"] == 3 and prof["s"]["null_rate"] == round(1 / 3, 6)


def test_sample_by_hash_is_deterministic(spark):
    df = spark.range(20_000).withColumnRenamed("id", "rid")
    a = as_set(sample_by_hash(df, "rid", 0.05), "rid")
    b = as_set(sample_by_hash(df, "rid", 0.05), "rid")
    assert a == b
    assert 700 < len(a) < 1300  # ~5% of 20,000
    assert sample_by_hash(df, "rid", None) is df
    with pytest.raises(ValueError):
        sample_by_hash(df, "rid", 0)
