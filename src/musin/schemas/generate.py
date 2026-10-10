"""Generate src/musin/schemas/tables.py from MusicBrainz's CreateTables.sql (F1.3).

    python -m musin.schemas.generate --sql <path/to/CreateTables.sql> \
        --commit <musicbrainz-server commit> --schema-sequence <n>

Dump files are headerless, so column order must come from the server's DDL at
the dump's schema version. Run this once per schema version and commit the
output; the pipeline never parses SQL at run time.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

# Tables in PRD §6.2, plus l_artist_release for OD7 coverage (F1.4).
TABLES: tuple[str, ...] = (
    "area",
    "artist",
    "artist_credit",
    "artist_credit_name",
    "artist_tag",
    "artist_type",
    "genre",
    "genre_alias",
    "l_artist_artist",
    "l_artist_recording",
    "l_artist_release",
    "link",
    "link_type",
    "medium",
    "recording",
    "release",
    "release_country",
    "release_group",
    "release_group_tag",
    "release_unknown_country",
    "tag",
    "track",
)

# Tables that ship in mbdump-derived.tar.bz2; every other table is in mbdump.tar.bz2.
DERIVED: frozenset[str] = frozenset({"tag", "artist_tag", "release_group_tag"})

# Postgres type -> Spark type name. Anything else stops the generator.
TYPE_MAP: dict[str, str] = {
    "SERIAL": "IntegerType",
    "INTEGER": "IntegerType",
    "SMALLINT": "ShortType",
    "BIGINT": "LongType",
    "TEXT": "StringType",
    "VARCHAR": "StringType",
    "CHAR": "StringType",
    "UUID": "StringType",
    "BOOLEAN": "BooleanType",
    "TIMESTAMP": "TimestampType",
    "DATE": "DateType",
}

COLUMN_RE = re.compile(r"^\s+([a-z][a-z0-9_]*)\s+([A-Za-z]+)\b")  # DDL mixes "UUID"/"uuid"
SQL_WORDS = frozenset({"check", "constraint", "primary", "unique", "and", "or", "not"})
CHECK_WORDS = frozenset({"IS", "AND", "OR", "NOT", "IN"})


def parse_table(sql: str, table: str) -> list[tuple[str, str]]:
    """Return [(column, postgres_base_type)] in DDL order for one table."""
    m = re.search(rf"^CREATE TABLE {table} \((.*?)^\);", sql, re.S | re.M)
    if not m:
        raise ValueError(f"CREATE TABLE {table} not found")
    cols = []
    for line in m.group(1).splitlines():
        cm = COLUMN_RE.match(line.split("--")[0])
        if not cm or cm.group(1) in SQL_WORDS:
            continue
        name, pg_type = cm.group(1), cm.group(2).upper()
        if pg_type in CHECK_WORDS:  # continuation line of a CHECK constraint
            continue
        if pg_type not in TYPE_MAP:
            raise ValueError(f"{table}.{name}: unmapped Postgres type {pg_type}")
        cols.append((name, pg_type))
    if not cols:
        raise ValueError(f"{table}: no columns parsed")
    names = [c for c, _ in cols]
    if len(names) != len(set(names)):
        raise ValueError(f"{table}: duplicate columns parsed: {names}")
    return cols


def render(sql: str, commit: str, schema_sequence: int) -> str:
    sha = hashlib.sha256(sql.encode()).hexdigest()
    parsed = {table: parse_table(sql, table) for table in TABLES}
    used = sorted({TYPE_MAP[t] for cols in parsed.values() for _, t in cols})
    out = [
        '"""Explicit Spark schemas for MusicBrainz dump tables. GENERATED: do not edit.',
        "",
        "Source: metabrainz/musicbrainz-server admin/sql/CreateTables.sql",
        f"  commit {commit}",
        f"  sha256 {sha}",
        "Regenerate with `python -m musin.schemas.generate` (see its docstring).",
        '"""',
        "",
        "from pyspark.sql.types import (",
        *[f"    {name}," for name in sorted([*used, "StructField", "StructType"])],
        ")",
        "",
        f"SCHEMA_SEQUENCE = {schema_sequence}",
        f'SOURCE_COMMIT = "{commit}"',
        "",
        "# Tables shipped in mbdump-derived.tar.bz2; all others are in mbdump.tar.bz2.",
        "DERIVED_TABLES = frozenset({" + ", ".join(f'"{t}"' for t in sorted(DERIVED)) + "})",
        "",
        "SCHEMAS: dict[str, StructType] = {",
    ]
    for table in TABLES:
        out.append(f'    "{table}": StructType(')
        out.append("        [")
        for name, pg_type in parsed[table]:
            spark_type = TYPE_MAP[pg_type]
            out.append(f'            StructField("{name}", {spark_type}()),  # {pg_type}')
        out.append("        ]")
        out.append("    ),")
    out.append("}")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sql", required=True, type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--schema-sequence", required=True, type=int)
    parser.add_argument("--out", type=Path, default=Path(__file__).with_name("tables.py"))
    args = parser.parse_args(argv)
    text = render(args.sql.read_text(encoding="utf-8"), args.commit, args.schema_sequence)
    args.out.write_text(text, encoding="utf-8", newline="\n")  # LF even on Windows
    print(f"wrote {args.out} ({len(TABLES)} tables)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
