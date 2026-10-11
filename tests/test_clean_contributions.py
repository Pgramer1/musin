"""Tests for musin.clean.contributions (F2.3) on tiny frames."""

import pytest

from musin.clean.contributions import (
    allowlist,
    collapse,
    relationships,
    release_to_recordings,
    resolve_link_types,
)
from musin.common.config import ConfigError

NAMES = {"producer": "PRODUCED_FOR", "instrument": "PERFORMED_FOR", "vocal": "PERFORMED_FOR"}


class _Cfg:
    def __init__(self, produced, performed):
        self.values = {
            "contributions.produced_for": produced,
            "contributions.performed_for": performed,
        }

    def get(self, key):
        return self.values[key]


@pytest.fixture
def link_type(spark):
    rows = [
        (141, "producer", "artist", "recording"),
        (148, "instrument", "artist", "recording"),
        (149, "vocal", "artist", "recording"),
        (151, "conductor", "artist", "recording"),
        (30, "producer", "artist", "release"),  # same name, other target
    ]
    return spark.createDataFrame(
        rows, "id int, name string, entity_type0 string, entity_type1 string"
    )


def test_allowlist_maps_names_and_rejects_overlap():
    assert allowlist(_Cfg(["producer"], ["instrument", "vocal"])) == NAMES
    with pytest.raises(ConfigError, match="two contribution types"):
        allowlist(_Cfg(["producer"], ["producer"]))
    with pytest.raises(ConfigError, match="empty"):
        allowlist(_Cfg([], ["vocal"]))


def test_resolve_link_types_by_target(link_type):
    rec = {
        r["name"]: r["link_type"]
        for r in resolve_link_types(link_type, NAMES, "recording").collect()
    }
    assert rec == {"producer": 141, "instrument": 148, "vocal": 149}
    with pytest.raises(ConfigError, match="instrument"):
        resolve_link_types(link_type, NAMES, "release")  # only 'producer' exists for releases


def test_relationships_keep_allowlisted_only(spark, link_type):
    link = spark.createDataFrame([(1, 141), (2, 148), (3, 151)], "id int, link_type int")
    rel = spark.createDataFrame(
        [(1, 10, 100), (2, 11, 100), (3, 12, 100)], "link int, entity0 int, entity1 int"
    )
    got = relationships(rel, link, resolve_link_types(link_type, NAMES, "recording"))
    assert sorted((r["artist"], r["target"], r["edge_type"]) for r in got.collect()) == [
        (10, 100, "PRODUCED_FOR"),
        (11, 100, "PERFORMED_FOR"),
    ]  # conductor (151) is not allowlisted


def test_release_to_recordings_distinct(spark):
    track = spark.createDataFrame([(100, 1), (100, 1), (101, 2)], "recording int, medium int")
    medium = spark.createDataFrame([(1, 7), (2, 7)], "id int, release int")
    pairs = sorted(tuple(r) for r in release_to_recordings(track, medium).collect())
    assert pairs == [(7, 100), (7, 101)]


def test_collapse_merges_names_and_prefers_recording_level(spark):
    rows = [
        (100, 11, "instrument", "PERFORMED_FOR", "recording"),
        (100, 11, "vocal", "PERFORMED_FOR", "release"),
        (100, 11, "producer", "PRODUCED_FOR", "release"),
        (101, 12, "vocal", "PERFORMED_FOR", "release"),
    ]
    df = spark.createDataFrame(
        rows, "recording_id int, artist_id int, name string, edge_type string, level string"
    )
    out = {(r["recording_id"], r["artist_id"], r["edge_type"]): r for r in collapse(df).collect()}
    assert len(out) == 3
    assert out[(100, 11, "PERFORMED_FOR")]["link_types"] == ["instrument", "vocal"]
    assert out[(100, 11, "PERFORMED_FOR")]["level"] == "recording"
    assert out[(100, 11, "PRODUCED_FOR")]["level"] == "release"
    assert dict(collapse(df).dtypes)["recording_id"] == "bigint"
