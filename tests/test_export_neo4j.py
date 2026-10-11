"""Tests for musin.export.neo4j_csv and musin.export.neo4j_check (F3.4)."""

import re
from pathlib import Path

import pytest
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from musin.common.provenance import fingerprint
from musin.export.neo4j_check import compare_rows, expected_missing, header_property
from musin.export.neo4j_csv import (
    HEADERS,
    artists_frame,
    edges_frame,
    field_type,
    graph_name,
    header_schema,
    newline_rows,
    read_back,
    write_csv,
)
from musin.graph.build import CO_CREDITED, FEATURED

PRD = Path(__file__).resolve().parents[1] / "PRD.md"
NODES = (
    "artist_id long, mbid string, name string, artist_type string, area_name string, "
    "begin_year int, recording_count long, primary_genre string, genre_votes long"
)


def test_graph_name():
    assert graph_name(None) == "full"
    assert graph_name(0.05) == "sample-f0.05"


def test_headers_match_prd_section_8_3():
    section = PRD.read_text(encoding="utf-8").split("**Import CSVs")[1].split("### 8.4")[0]
    documented = set(re.findall(r"`([^`]*(?:LABEL|TYPE))`", section))
    for stem, header in HEADERS.items():
        assert ",".join(header) in documented, stem


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("artistId:ID(Artist){id-type:long}", LongType()),
        ("name:ID(Genre)", StringType()),
        (":START_ID(Artist)", LongType()),
        (":END_ID(Genre)", StringType()),
        ("beginYear:int", IntegerType()),
        ("recordingCount:long", LongType()),
        ("share:double", DoubleType()),
        ("areaName", StringType()),
        (":LABEL", StringType()),
        (":TYPE", StringType()),
    ],
)
def test_field_type(field, expected):
    assert field_type(field) == expected


def test_field_type_rejects_unknown_suffix():
    with pytest.raises(ValueError):
        field_type("when:datetime")


@pytest.fixture
def hazardous_nodes(spark):
    rows = [
        (1, "m1", 'Andrew "Big Voice" Odom', "Person", "Washington, D.C.", 1950, 3, "blues", 4),
        (2, "m2", "Back\\slash", None, None, None, 1, None, None),
        (3, "m3", "  padded  ", "Group", "", 1999, 2, "rock", 1),  # empty string area
        (4, "m4", '初音ミク, "x"', "Character", "日本", -50, 7, "pop", 2),
        (5, "m5", '"', "Other", '""', 0, 1, None, None),
    ]
    return spark.createDataFrame(rows, NODES)


def test_csv_round_trip_preserves_hazards(spark, tmp_path, hazardous_nodes):
    df = artists_frame(hazardous_nodes)
    parts = write_csv(df, HEADERS["artists"], tmp_path / "artists", df.count())
    assert parts and (tmp_path / "artists.header.csv").read_text().startswith("artistId:ID(Artist)")
    back = read_back(spark, tmp_path / "artists", HEADERS["artists"])
    assert fingerprint(back) == fingerprint(df)
    areas = {r["artist_id"]: r["area_name"] for r in df.collect()}
    assert areas[3] is None  # empty string exported as a missing property
    names = {r["c0"]: r["c2"] for r in back.collect()}
    assert names[3] == "  padded  "  # Spark would trim without the explicit options
    assert names[2] == "Back\\slash"
    raw = "".join(p.read_text(encoding="utf-8") for p in parts)
    assert '"Andrew ""Big Voice"" Odom"' in raw  # RFC 4180 quote doubling, not \"


def test_nulls_are_empty_unquoted_fields(spark, tmp_path, hazardous_nodes):
    df = artists_frame(hazardous_nodes.filter("artist_id = 2"))
    parts = write_csv(df, HEADERS["artists"], tmp_path / "a", 1)
    line = parts[0].read_text(encoding="utf-8").strip()
    assert line == "2,m2,Back\\slash,,,,1,,Artist"  # empty field -> Neo4j omits the property


def test_edges_frame_and_newline_guard(spark):
    edges = spark.createDataFrame(
        [(1, 2, CO_CREDITED, 3, 1990, 1999), (2, 1, FEATURED, 1, None, None)],
        "src_id long, dst_id long, edge_type string, weight long, first_year int, last_year int",
    )
    feat = edges_frame(edges, FEATURED)
    assert [tuple(r) for r in feat.collect()] == [(2, 1, 1, None, None, FEATURED)]
    declared = header_schema(HEADERS["edges_featured"])
    assert [f.dataType for f in feat.schema] == [f.dataType for f in declared]
    assert newline_rows(feat) == 0
    bad = spark.createDataFrame([("a\nb",), ("ok",)], "name string")
    assert newline_rows(bad) == 1


def test_expected_missing_and_header_property():
    assert header_property("beginYear:int") == "beginYear"
    assert header_property(":LABEL") == ""
    export = {
        "files": {
            "artists": {"nulls": {"areaName": 5, "beginYear:int": 2}},
            "edges_featured": {"nulls": {"firstYear:int": 1, ":TYPE": 0}},
        }
    }
    assert expected_missing(export) == {
        "artists.areaName": 5,
        "artists.beginYear": 2,
        "edges_featured.firstYear": 1,
    }


def test_compare_rows():
    expected = {1: {"name": "A", "areaName": None}, 2: {"name": "B", "areaName": "X"}}
    actual = {1: {"name": "A"}, 2: {"name": "B", "areaName": "Y"}}  # missing prop == None
    diffs = compare_rows(expected, actual, "artist")
    assert diffs == ["artist 2: {'areaName': ('X', 'Y')}"]
    assert compare_rows({3: {"name": "C"}}, {}, "artist") == ["artist 3 missing in Neo4j"]
