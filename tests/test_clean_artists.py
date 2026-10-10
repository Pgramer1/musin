"""Tests for musin.clean.artists (F2.1) on tiny frames and files."""

from pathlib import Path

import pytest

from musin.clean.artists import (
    SpecialArtistsError,
    clean_artists,
    load_special_artists,
    resolve_special_artists,
)

REPO = Path(__file__).resolve().parents[1]
VA = "89ad4ac3-39f7-470e-963a-56509c546377"
UNKNOWN = "125ec42a-7229-4250-afc5-e057484327fe"
OTHER = "10adbe5e-a2c0-4bf3-8249-2b4cbf6e6ca8"

ARTIST_SCHEMA = (
    "id int, gid string, name string, comment string, type int, area int, begin_date_year short"
)


@pytest.fixture
def artist(spark):
    rows = [
        (1, VA, "Various Artists", "add compilations to this artist", 6, None, None),
        (97546, UNKNOWN, "[unknown]", "Special Purpose Artist", 6, None, None),
        (4, OTHER, "Massive Attack", "", 2, 221, 1988),
        (5, "00000000-0000-4000-8000-000000000005", "Heraclitus", "", 1, None, -535),
        (6, "00000000-0000-4000-8000-000000000006", "Future Typo", "Rapper", 1, 999, 4428),
    ]
    return spark.createDataFrame(rows, ARTIST_SCHEMA)


def write_csv(tmp_path, text):
    path = tmp_path / "special.csv"
    path.write_text(text, encoding="utf-8")
    return path


def test_committed_special_artists_file_is_valid():
    rows = load_special_artists(REPO / "config" / "special_artists.csv")
    assert len(rows) >= 10
    assert {VA, UNKNOWN} <= {r["mbid"] for r in rows}


@pytest.mark.parametrize(
    "text, message",
    [
        ("mbid,name\n", "header"),
        ("mbid,name,reason\nnot-a-uuid,X,y\n", "malformed"),
        (f"mbid,name,reason\n{VA},Various Artists,a\n{VA},Various Artists,b\n", "duplicate"),
        (f"mbid,name,reason\n{VA},Various Artists,\n", "empty"),
        ("mbid,name,reason\n", "no special"),
    ],
)
def test_load_special_artists_rejects_bad_files(tmp_path, text, message):
    with pytest.raises(SpecialArtistsError, match=message):
        load_special_artists(write_csv(tmp_path, text))


def test_resolve_special_artists_finds_ids(artist):
    specials = [
        {"mbid": VA, "name": "Various Artists", "reason": "r"},
        {"mbid": UNKNOWN, "name": "[unknown]", "reason": "r"},
    ]
    got = [r.asDict() for r in resolve_special_artists(specials, artist).collect()]
    assert [(r["artist_id"], r["name"]) for r in got] == [
        (1, "Various Artists"),
        (97546, "[unknown]"),
    ]


def test_resolve_special_artists_fails_on_missing_or_renamed(artist):
    missing = [{"mbid": "00000000-0000-4000-8000-0000000000ff", "name": "X", "reason": "r"}]
    with pytest.raises(SpecialArtistsError, match="not in the dump"):
        resolve_special_artists(missing, artist)
    renamed = [{"mbid": VA, "name": "Various", "reason": "r"}]
    with pytest.raises(SpecialArtistsError, match="names differ"):
        resolve_special_artists(renamed, artist)


def test_clean_artists_filters_types_and_years(spark, artist):
    artist_type = spark.createDataFrame(
        [(1, "Person"), (2, "Group"), (6, "Other")], "id int, name string"
    )
    area = spark.createDataFrame([(221, "United Kingdom")], "id int, name string")
    special = spark.createDataFrame([(1,), (97546,)], "artist_id long")

    out = clean_artists(artist, artist_type, area, special, max_begin_year=2026)
    rows = {r["artist_id"]: r.asDict() for r in out.collect()}

    assert set(rows) == {4, 5, 6}  # both special artists removed
    assert dict(out.dtypes)["artist_id"] == "bigint" and dict(out.dtypes)["begin_year"] == "int"
    assert rows[4] == {
        "artist_id": 4,
        "mbid": OTHER,
        "name": "Massive Attack",
        "disambiguation": None,  # empty comment -> null
        "artist_type": "Group",
        "area_name": "United Kingdom",
        "begin_year": 1988,
    }
    assert rows[5]["begin_year"] == -535  # historical year kept
    assert rows[6]["begin_year"] is None  # future year is an entry error
    assert rows[6]["area_name"] is None  # area id without a match stays null
    assert rows[6]["disambiguation"] == "Rapper"
