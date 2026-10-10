"""Tests for musin.ingest.pgcopy: COPY text-format parsing on hand-made lines."""

from datetime import datetime

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from musin.ingest.pgcopy import parse_copy

SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("year", ShortType()),
        StructField("ended", BooleanType()),
        StructField("last_updated", TimestampType()),
    ]
)


def parse(spark, lines, schema=SCHEMA):
    df = spark.createDataFrame([(line,) for line in lines], ["value"])
    return parse_copy(df, schema, "t").collect()


def test_types_and_nulls(spark):
    rows = parse(
        spark,
        [
            "1\tRadiohead\t1985\tf\t2011-05-16 16:32:11.963929+00",
            "2\t\\N\t\\N\t\\N\t\\N",
        ],
    )
    assert rows[0].asDict() == {
        "id": 1,
        "name": "Radiohead",
        "year": 1985,
        "ended": False,
        "last_updated": datetime(2011, 5, 16, 16, 32, 11, 963929),
    }
    assert rows[1].asDict() == {
        "id": 2,
        "name": None,
        "year": None,
        "ended": None,
        "last_updated": None,
    }


def test_empty_string_is_not_null(spark):
    (row,) = parse(spark, ["3\t\t\\N\tt\t\\N"])
    assert row["name"] == ""
    assert row["ended"] is True


@pytest.mark.parametrize(
    ("escaped", "expected"),
    [
        ("Line\\nBreak", "Line\nBreak"),
        ("Tab\\there", "Tab\there"),
        ("Back\\\\slash", "Back\\slash"),
        # escaped backslash followed by "n" must NOT become a newline
        ("C:\\\\new", "C:\\new"),
        ("\\\\N", "\\N"),  # the literal text "\N", not a null
        ('Quote "unchanged"', 'Quote "unchanged"'),
        ("Björk feat. 宇多田ヒカル", "Björk feat. 宇多田ヒカル"),
    ],
)
def test_unescape(spark, escaped, expected):
    (row,) = parse(spark, [f"1\t{escaped}\t\\N\t\\N\t\\N"])
    assert row["name"] == expected


def test_wrong_field_count_fails(spark):
    with pytest.raises(Exception, match="expected 5 fields, got 4"):
        parse(spark, ["1\tonly\tfour\tf"])


def test_trailing_empty_field_is_counted(spark):
    schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
    (row,) = parse(spark, ["7\t"], schema)
    assert row.asDict() == {"a": 7, "b": ""}


def test_bad_boolean_fails(spark):
    with pytest.raises(Exception, match="bad boolean"):
        parse(spark, ["1\tx\t\\N\tyes\t\\N"])


def test_bad_integer_fails(spark):
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        parse(spark, ["one\tx\t\\N\t\\N\t\\N"])
