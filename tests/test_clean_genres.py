"""Tests for musin.clean.genres (F2.5) on tiny frames and files."""

from pathlib import Path

import pytest

from musin.clean.genres import (
    check_mapping,
    choose_source,
    load_mapping,
    main_artists_of_release_groups,
    parent_tags,
    primary_genres,
    profiles,
    tag_votes,
)
from musin.common.config import ConfigError

REPO = Path(__file__).resolve().parents[1]
FEAT = r"(?i)\b(feat|ft|featuring)\b"


def test_committed_mapping_is_valid():
    mapping = load_mapping(REPO / "config" / "genre_mapping.csv")
    parents = {p for p in mapping.values() if p}
    assert len(mapping) >= 2000
    assert len(parents) == 15  # OD4: 12-15 parent genres (D-024)
    assert mapping["pop rock"] == "rock" and mapping["folk metal"] == "metal"
    assert mapping["instrumental"] == ""  # not a style


@pytest.mark.parametrize(
    "text, message",
    [
        ("genre,parent_genre\n", "header"),
        ("genre,parent_genre,basis\nrock,rock,rule\nrock,pop,rule\n", "duplicate"),
        ("genre,parent_genre,basis\n,rock,rule\n", "empty"),
    ],
)
def test_load_mapping_rejects_bad_files(tmp_path, text, message):
    path = tmp_path / "m.csv"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match=message):
        load_mapping(path)


def test_check_mapping_requires_exact_genre_set():
    check_mapping({"rock": "rock", "noise": ""}, {"rock", "noise"})
    with pytest.raises(ConfigError, match="not listed"):
        check_mapping({"rock": "rock"}, {"rock", "jazz"})


def test_parent_tags_and_votes(spark):
    gtags = spark.createDataFrame(
        [(1, "rock"), (2, "hard rock"), (2, "rock"), (3, "noise")], "tag int, genre_name string"
    )
    mapping = spark.createDataFrame(
        [("rock", "rock"), ("hard rock", "rock")], "genre_name string, parent_genre string"
    )
    ptags = parent_tags(gtags, mapping)
    assert sorted(tuple(r) for r in ptags.collect()) == [(1, "rock"), (2, "rock")]  # tag 2 once
    tags = spark.createDataFrame(
        [(10, 1, 3), (10, 2, 2), (10, 3, 9), (11, 1, -1)], "artist int, tag int, count int"
    )
    got = {
        (r["artist"], r["parent_genre"]): r["votes"]
        for r in tag_votes(tags, "artist", ptags).collect()
    }
    assert got == {(10, "rock"): 5}  # noise unmapped; non-positive votes ignored


def test_release_group_genre_goes_to_main_artists_only(spark):
    rg = spark.createDataFrame([(500, 1)], "id int, artist_credit int")
    acn = spark.createDataFrame(
        [(1, 0, 10, " feat. "), (1, 1, 11, "")],
        "artist_credit int, position short, artist int, join_phrase string",
    )
    got = sorted(tuple(r) for r in main_artists_of_release_groups(rg, acn, FEAT).collect())
    assert got == [(500, 10)]  # featured guest 11 does not inherit the album's tags


def test_choose_source_artist_first(spark):
    schema = "artist int, parent_genre string, votes long"
    own = spark.createDataFrame([(10, "rock", 2)], schema)
    rg = spark.createDataFrame([(10, "pop", 50), (11, "jazz", 4)], schema)
    got = sorted(tuple(r) for r in choose_source(own, rg).collect())
    assert got == [(10, "rock", 2, "artist"), (11, "jazz", 4, "release_group")]


def test_profiles_and_primary_with_tie(spark):
    votes = spark.createDataFrame(
        [
            (10, "rock", 3, "artist"),
            (10, "pop", 1, "artist"),
            (11, "rock", 2, "release_group"),
            (11, "hip hop", 2, "release_group"),  # tie -> alphabetical: hip hop
        ],
        "artist_id long, parent_genre string, votes long, source string",
    )
    prof = profiles(votes)
    shares = {(r["artist_id"], r["parent_genre"]): r["share"] for r in prof.collect()}
    assert shares[(10, "rock")] == pytest.approx(0.75) and shares[(11, "rock")] == pytest.approx(
        0.5
    )
    prim = {r["artist_id"]: r.asDict() for r in primary_genres(prof).collect()}
    assert prim[10] == {
        "artist_id": 10,
        "primary_genre": "rock",
        "genre_votes": 4,
        "source": "artist",
        "tied": False,
    }
    assert prim[11]["primary_genre"] == "hip hop" and prim[11]["tied"] is True
