"""Tests for the schema generator and the committed, generated schema module."""

import pytest

from musin.schemas.generate import TABLES, parse_table
from musin.schemas.tables import DERIVED_TABLES, SCHEMA_SEQUENCE, SCHEMAS

DDL = """
CREATE TABLE area ( -- replicate (verbose)
    id                  SERIAL, -- PK
    gid                 uuid NOT NULL,
    name                VARCHAR NOT NULL,
    ended               BOOLEAN NOT NULL DEFAULT FALSE
      CHECK (
        (
          -- If any end date fields are not null, then ended must be true
          (end_date_year IS NOT NULL OR
           end_date_month IS NOT NULL) AND
          ended = TRUE
        ) OR (
          end_date_year IS NULL
        )
      ),
    comment             VARCHAR(255) NOT NULL DEFAULT ''
);

CREATE TABLE weird (
    id                  SERIAL,
    shape               POINT
);
"""


def test_parse_table_handles_lowercase_types_and_checks():
    assert parse_table(DDL, "area") == [
        ("id", "SERIAL"),
        ("gid", "UUID"),
        ("name", "VARCHAR"),
        ("ended", "BOOLEAN"),
        ("comment", "VARCHAR"),
    ]


def test_parse_table_rejects_unknown_type():
    with pytest.raises(ValueError, match="unmapped Postgres type POINT"):
        parse_table(DDL, "weird")


def test_parse_table_missing():
    with pytest.raises(ValueError, match="not found"):
        parse_table(DDL, "nope")


def test_generated_module_covers_every_table():
    assert set(SCHEMAS) == set(TABLES)
    assert SCHEMA_SEQUENCE == 31
    assert DERIVED_TABLES <= set(SCHEMAS)


@pytest.mark.parametrize(
    ("table", "first_columns"),
    [
        ("artist", ["id", "gid", "name", "sort_name", "begin_date_year"]),
        ("artist_credit_name", ["artist_credit", "position", "artist", "name", "join_phrase"]),
        ("l_artist_recording", ["id", "link", "entity0", "entity1", "edits_pending"]),
        ("release_country", ["release", "country", "date_year", "date_month", "date_day"]),
    ],
)
def test_generated_column_order(table, first_columns):
    assert SCHEMAS[table].fieldNames()[: len(first_columns)] == first_columns
