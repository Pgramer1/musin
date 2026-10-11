"""Tests for musin.clean.credits (F2.2) and musin.common.layers on tiny frames."""

from pathlib import Path

import pytest

from musin.clean.credits import collapse_repeats, credit_roles, drop_special, recording_credits
from musin.common.layers import layer_path, sample_tag

FEAT = r"(?i)\b(feat|ft|featuring)\b"
ACN_SCHEMA = "artist_credit int, position short, artist int, join_phrase string"
UNKNOWN = 97546


@pytest.fixture
def acn(spark):
    rows = [
        # 1: "A feat. B & C" -> B and C both come after the featuring phrase
        (1, 0, 10, " feat. "),
        (1, 1, 11, " & "),
        (1, 2, 12, ""),
        # 2: "A & B feat. C"
        (2, 0, 10, " & "),
        (2, 1, 11, " Feat. "),
        (2, 2, 12, ""),
        # 3: "A feat." with nobody after it -> A stays main
        (3, 0, 13, " feat."),
        # 4: "[unknown] feat. X" -> X featured, placeholder removed afterwards
        (4, 0, UNKNOWN, " ft. "),
        (4, 1, 14, ""),
        # 5: ambiguous phrases count as main (PRD §8.2)
        (5, 0, 15, " with "),
        (5, 1, 16, " x "),
        (5, 2, 17, " vs. "),
        (5, 3, 18, ""),
        # 6: same artist listed twice, once after "featuring"
        (6, 0, 19, " featuring "),
        (6, 1, 20, " & "),
        (6, 2, 19, ""),
        # 7: "featuring" inside a longer phrase, and "Left" must not match "ft"
        (7, 0, 21, " (featuring "),
        (7, 1, 22, ") Left "),
        (7, 2, 23, ""),
    ]
    return spark.createDataFrame(rows, ACN_SCHEMA)


def roles_of(df, credit):
    return {
        (r["position"], r["artist"]): r["role"]
        for r in df.filter(f"artist_credit = {credit}").collect()
    }


def test_featured_after_matching_phrase(acn):
    roles = credit_roles(acn, FEAT)
    assert roles_of(roles, 1) == {(0, 10): "main", (1, 11): "featured", (2, 12): "featured"}
    assert roles_of(roles, 2) == {(0, 10): "main", (1, 11): "main", (2, 12): "featured"}
    assert roles_of(roles, 3) == {(0, 13): "main"}
    assert roles_of(roles, 5) == {
        (0, 15): "main",
        (1, 16): "main",
        (2, 17): "main",
        (3, 18): "main",
    }
    assert roles_of(roles, 7) == {(0, 21): "main", (1, 22): "featured", (2, 23): "featured"}
    assert roles.count() == acn.count()


def test_special_removed_after_roles(spark, acn):
    special = spark.createDataFrame([(UNKNOWN,)], "artist_id long")
    kept = drop_special(credit_roles(acn, FEAT), special)
    assert roles_of(kept, 4) == {(1, 14): "featured"}  # not promoted to main
    assert kept.filter(f"artist = {UNKNOWN}").count() == 0


def test_repeated_artist_collapsed(acn):
    collapsed = collapse_repeats(credit_roles(acn, FEAT))
    assert roles_of(collapsed, 6) == {(0, 19): "main", (1, 20): "featured"}
    assert collapsed.groupBy("artist_credit", "artist").count().filter("count > 1").count() == 0


def test_recording_credits_types_and_join(spark, acn):
    recording = spark.createDataFrame([(100, 1), (101, 1), (102, 3)], "id int, artist_credit int")
    out = recording_credits(recording, collapse_repeats(credit_roles(acn, FEAT)))
    assert dict(out.dtypes) == {
        "recording_id": "bigint",
        "artist_id": "bigint",
        "position": "int",
        "role": "string",
    }
    got = sorted((r["recording_id"], r["artist_id"], r["role"]) for r in out.collect())
    assert got == [
        (100, 10, "main"),
        (100, 11, "featured"),
        (100, 12, "featured"),
        (101, 10, "main"),
        (101, 11, "featured"),
        (101, 12, "featured"),
        (102, 13, "main"),
    ]


class _Cfg:
    data_root = Path("/data")

    def data_path(self, layer):
        return self.data_root / f"{layer}/"

    def get(self, key):
        return {"paths.clean": "clean/"}[key]


def test_layer_path_full_and_sample():
    assert layer_path(_Cfg(), "clean", None) == Path("/data/clean")
    assert layer_path(_Cfg(), "clean", 0.05) == Path("/data/sample/f0.05/clean")
    assert sample_tag(0.1) == "f0.1"
    with pytest.raises(ValueError):
        sample_tag(1.0)
