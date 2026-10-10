"""Explicit Spark schemas for MusicBrainz dump tables. GENERATED: do not edit.

Source: metabrainz/musicbrainz-server admin/sql/CreateTables.sql
  commit c93f815cbfe10f1c5ba0b752a8ef878a15ee68df
  sha256 92d1981a0d4f5032bca1565d27fafa8a0eac4d11b7f34feab71bfde8a00ecb13
Regenerate with `python -m musin.schemas.generate` (see its docstring).
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_SEQUENCE = 31
SOURCE_COMMIT = "c93f815cbfe10f1c5ba0b752a8ef878a15ee68df"

# Tables shipped in mbdump-derived.tar.bz2; all others are in mbdump.tar.bz2.
DERIVED_TABLES = frozenset({"artist_tag", "release_group_tag", "tag"})

SCHEMAS: dict[str, StructType] = {
    "area": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("type", IntegerType()),  # INTEGER
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("begin_date_year", ShortType()),  # SMALLINT
            StructField("begin_date_month", ShortType()),  # SMALLINT
            StructField("begin_date_day", ShortType()),  # SMALLINT
            StructField("end_date_year", ShortType()),  # SMALLINT
            StructField("end_date_month", ShortType()),  # SMALLINT
            StructField("end_date_day", ShortType()),  # SMALLINT
            StructField("ended", BooleanType()),  # BOOLEAN
            StructField("comment", StringType()),  # VARCHAR
        ]
    ),
    "artist": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("sort_name", StringType()),  # VARCHAR
            StructField("begin_date_year", ShortType()),  # SMALLINT
            StructField("begin_date_month", ShortType()),  # SMALLINT
            StructField("begin_date_day", ShortType()),  # SMALLINT
            StructField("end_date_year", ShortType()),  # SMALLINT
            StructField("end_date_month", ShortType()),  # SMALLINT
            StructField("end_date_day", ShortType()),  # SMALLINT
            StructField("type", IntegerType()),  # INTEGER
            StructField("area", IntegerType()),  # INTEGER
            StructField("gender", IntegerType()),  # INTEGER
            StructField("comment", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("ended", BooleanType()),  # BOOLEAN
            StructField("begin_area", IntegerType()),  # INTEGER
            StructField("end_area", IntegerType()),  # INTEGER
        ]
    ),
    "artist_credit": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("name", StringType()),  # VARCHAR
            StructField("artist_count", ShortType()),  # SMALLINT
            StructField("ref_count", IntegerType()),  # INTEGER
            StructField("created", TimestampType()),  # TIMESTAMP
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("gid", StringType()),  # UUID
        ]
    ),
    "artist_credit_name": StructType(
        [
            StructField("artist_credit", IntegerType()),  # INTEGER
            StructField("position", ShortType()),  # SMALLINT
            StructField("artist", IntegerType()),  # INTEGER
            StructField("name", StringType()),  # VARCHAR
            StructField("join_phrase", StringType()),  # TEXT
        ]
    ),
    "artist_tag": StructType(
        [
            StructField("artist", IntegerType()),  # INTEGER
            StructField("tag", IntegerType()),  # INTEGER
            StructField("count", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
        ]
    ),
    "artist_type": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("name", StringType()),  # VARCHAR
            StructField("parent", IntegerType()),  # INTEGER
            StructField("child_order", IntegerType()),  # INTEGER
            StructField("description", StringType()),  # TEXT
            StructField("gid", StringType()),  # UUID
        ]
    ),
    "genre": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("comment", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
        ]
    ),
    "genre_alias": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("genre", IntegerType()),  # INTEGER
            StructField("name", StringType()),  # VARCHAR
            StructField("locale", StringType()),  # TEXT
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("type", IntegerType()),  # INTEGER
            StructField("sort_name", StringType()),  # VARCHAR
            StructField("begin_date_year", ShortType()),  # SMALLINT
            StructField("begin_date_month", ShortType()),  # SMALLINT
            StructField("begin_date_day", ShortType()),  # SMALLINT
            StructField("end_date_year", ShortType()),  # SMALLINT
            StructField("end_date_month", ShortType()),  # SMALLINT
            StructField("end_date_day", ShortType()),  # SMALLINT
            StructField("primary_for_locale", BooleanType()),  # BOOLEAN
            StructField("ended", BooleanType()),  # BOOLEAN
        ]
    ),
    "l_artist_artist": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("link", IntegerType()),  # INTEGER
            StructField("entity0", IntegerType()),  # INTEGER
            StructField("entity1", IntegerType()),  # INTEGER
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("link_order", IntegerType()),  # INTEGER
            StructField("entity0_credit", StringType()),  # TEXT
            StructField("entity1_credit", StringType()),  # TEXT
        ]
    ),
    "l_artist_recording": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("link", IntegerType()),  # INTEGER
            StructField("entity0", IntegerType()),  # INTEGER
            StructField("entity1", IntegerType()),  # INTEGER
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("link_order", IntegerType()),  # INTEGER
            StructField("entity0_credit", StringType()),  # TEXT
            StructField("entity1_credit", StringType()),  # TEXT
        ]
    ),
    "l_artist_release": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("link", IntegerType()),  # INTEGER
            StructField("entity0", IntegerType()),  # INTEGER
            StructField("entity1", IntegerType()),  # INTEGER
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("link_order", IntegerType()),  # INTEGER
            StructField("entity0_credit", StringType()),  # TEXT
            StructField("entity1_credit", StringType()),  # TEXT
        ]
    ),
    "link": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("link_type", IntegerType()),  # INTEGER
            StructField("begin_date_year", ShortType()),  # SMALLINT
            StructField("begin_date_month", ShortType()),  # SMALLINT
            StructField("begin_date_day", ShortType()),  # SMALLINT
            StructField("end_date_year", ShortType()),  # SMALLINT
            StructField("end_date_month", ShortType()),  # SMALLINT
            StructField("end_date_day", ShortType()),  # SMALLINT
            StructField("attribute_count", IntegerType()),  # INTEGER
            StructField("created", TimestampType()),  # TIMESTAMP
            StructField("ended", BooleanType()),  # BOOLEAN
        ]
    ),
    "link_type": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("parent", IntegerType()),  # INTEGER
            StructField("child_order", IntegerType()),  # INTEGER
            StructField("gid", StringType()),  # UUID
            StructField("entity_type0", StringType()),  # VARCHAR
            StructField("entity_type1", StringType()),  # VARCHAR
            StructField("name", StringType()),  # VARCHAR
            StructField("description", StringType()),  # TEXT
            StructField("link_phrase", StringType()),  # VARCHAR
            StructField("reverse_link_phrase", StringType()),  # VARCHAR
            StructField("long_link_phrase", StringType()),  # VARCHAR
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("is_deprecated", BooleanType()),  # BOOLEAN
            StructField("has_dates", BooleanType()),  # BOOLEAN
            StructField("entity0_cardinality", ShortType()),  # SMALLINT
            StructField("entity1_cardinality", ShortType()),  # SMALLINT
        ]
    ),
    "medium": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("release", IntegerType()),  # INTEGER
            StructField("position", IntegerType()),  # INTEGER
            StructField("format", IntegerType()),  # INTEGER
            StructField("name", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("track_count", IntegerType()),  # INTEGER
            StructField("gid", StringType()),  # UUID
        ]
    ),
    "recording": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("artist_credit", IntegerType()),  # INTEGER
            StructField("length", IntegerType()),  # INTEGER
            StructField("comment", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("video", BooleanType()),  # BOOLEAN
        ]
    ),
    "release": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("artist_credit", IntegerType()),  # INTEGER
            StructField("release_group", IntegerType()),  # INTEGER
            StructField("status", IntegerType()),  # INTEGER
            StructField("packaging", IntegerType()),  # INTEGER
            StructField("language", IntegerType()),  # INTEGER
            StructField("script", IntegerType()),  # INTEGER
            StructField("barcode", StringType()),  # VARCHAR
            StructField("comment", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("quality", ShortType()),  # SMALLINT
            StructField("last_updated", TimestampType()),  # TIMESTAMP
        ]
    ),
    "release_country": StructType(
        [
            StructField("release", IntegerType()),  # INTEGER
            StructField("country", IntegerType()),  # INTEGER
            StructField("date_year", ShortType()),  # SMALLINT
            StructField("date_month", ShortType()),  # SMALLINT
            StructField("date_day", ShortType()),  # SMALLINT
        ]
    ),
    "release_group": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("name", StringType()),  # VARCHAR
            StructField("artist_credit", IntegerType()),  # INTEGER
            StructField("type", IntegerType()),  # INTEGER
            StructField("comment", StringType()),  # VARCHAR
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
        ]
    ),
    "release_group_tag": StructType(
        [
            StructField("release_group", IntegerType()),  # INTEGER
            StructField("tag", IntegerType()),  # INTEGER
            StructField("count", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
        ]
    ),
    "release_unknown_country": StructType(
        [
            StructField("release", IntegerType()),  # INTEGER
            StructField("date_year", ShortType()),  # SMALLINT
            StructField("date_month", ShortType()),  # SMALLINT
            StructField("date_day", ShortType()),  # SMALLINT
        ]
    ),
    "tag": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("name", StringType()),  # VARCHAR
            StructField("ref_count", IntegerType()),  # INTEGER
        ]
    ),
    "track": StructType(
        [
            StructField("id", IntegerType()),  # SERIAL
            StructField("gid", StringType()),  # UUID
            StructField("recording", IntegerType()),  # INTEGER
            StructField("medium", IntegerType()),  # INTEGER
            StructField("position", IntegerType()),  # INTEGER
            StructField("number", StringType()),  # TEXT
            StructField("name", StringType()),  # VARCHAR
            StructField("artist_credit", IntegerType()),  # INTEGER
            StructField("length", IntegerType()),  # INTEGER
            StructField("edits_pending", IntegerType()),  # INTEGER
            StructField("last_updated", TimestampType()),  # TIMESTAMP
            StructField("is_data_track", BooleanType()),  # BOOLEAN
        ]
    ),
}
