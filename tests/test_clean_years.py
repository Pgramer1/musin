"""Tests for musin.clean.years (F2.4) on tiny frames."""

import pytest

from musin.clean.years import recording_first_years, release_years

DATE = "release int, date_year short"


@pytest.fixture
def dates(spark):
    rc = spark.createDataFrame(
        [
            (1, 1999),
            (1, 1997),  # release 1: earliest country date wins
            (2, None),  # undated event
            (3, 206),  # typo: before the phonograph
            (3, 2001),
            (4, 2027),  # scheduled after the snapshot year only
            (5, 1878),  # earliest possible recordings are kept
        ],
        DATE,
    )
    ru = spark.createDataFrame([(1, 1995), (2, 2010)], DATE)
    return rc, ru


def years_of(df):
    return {r["release"]: r["year"] for r in df.collect()}


def test_release_years_bounded(dates):
    got = years_of(release_years(*dates, min_year=1877, max_year=2026))
    assert got == {1: 1995, 2: 2010, 3: 2001, 5: 1878}  # 4 has no counted date


def test_release_years_unbounded_matches_raw(dates):
    got = years_of(release_years(*dates, min_year=None, max_year=None))
    assert got == {1: 1995, 2: 2010, 3: 206, 4: 2027, 5: 1878}


def test_recording_first_years_takes_earliest_release(spark, dates):
    rel = release_years(*dates, min_year=1877, max_year=2026)
    track = spark.createDataFrame(
        [(100, 10), (100, 30), (101, 40), (102, 99)], "recording int, medium int"
    )
    medium = spark.createDataFrame([(10, 2), (30, 3), (40, 4)], "id int, release int")
    got = {
        r["recording"]: r["first_year"] for r in recording_first_years(track, medium, rel).collect()
    }
    # 100 is on releases 2 (2010) and 3 (2001 after ignoring 206); 101 only on the
    # 2027 release -> absent (null later); 102 on an unknown medium -> absent
    assert got == {100: 2001}
