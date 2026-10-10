"""Parse PostgreSQL COPY text format with built-in Spark functions (F1.3).

MusicBrainz dump files are `COPY ... TO` output: one row per line, fields
separated by a literal TAB, `\\N` for NULL, and backslash escapes inside values
(`\\\\`, `\\n`, `\\t`, `\\r`, `\\b`, `\\f`, `\\v`). A raw TAB or newline never occurs
inside a value, so splitting a line on TAB is exact. No Python UDFs are used.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructType

NULL = "\\N"
# Escape sequence -> character. `\\` is handled separately (see unescape).
ESCAPES: tuple[tuple[str, str], ...] = (
    ("\\n", "\n"),
    ("\\t", "\t"),
    ("\\r", "\r"),
    ("\\b", "\b"),
    ("\\f", "\f"),
    ("\\v", "\v"),
)
# Postgres text can never contain NUL, so it is a safe stand-in for `\\`.
PLACEHOLDER = "\x00"


def unescape(col: Column) -> Column:
    """Decode COPY backslash escapes in a string column.

    `\\\\` becomes a placeholder first, so `\\\\n` (an escaped backslash then
    "n") stays a backslash + "n" instead of turning into a newline.
    """
    out = F.replace(col, F.lit("\\\\"), F.lit(PLACEHOLDER))
    for seq, char in ESCAPES:
        out = F.replace(out, F.lit(seq), F.lit(char))
    out = F.replace(out, F.lit(PLACEHOLDER), F.lit("\\"))
    return F.when(col.contains("\\"), out).otherwise(col)


def parse_copy(lines: DataFrame, schema: StructType, table: str) -> DataFrame:
    """Turn raw COPY lines into typed columns.

    Input: one string column `value` (one dump line per row).
    Output: the columns of `schema`, in order, with `\\N` -> null, escapes
    decoded, and values cast to the schema types.

    A line whose field count differs from the schema raises an error (via the
    first column, so the check cannot be optimised away). Casts rely on
    spark.sql.ansi.enabled=true to raise on malformed values; booleans other
    than "t"/"f" raise explicitly.
    """
    n = len(schema.fields)
    fields = F.split(F.col("value"), "\t", -1)
    bad_width = F.raise_error(
        F.concat(
            F.lit(f"{table}: expected {n} fields, got "),
            F.size(fields).cast("string"),
            F.lit(" in line: "),
            F.substring(F.col("value"), 1, 200),
        )
    )
    cols = []
    for i, field in enumerate(schema.fields):
        raw = fields.getItem(i)
        if i == 0:
            raw = F.when(F.size(fields) == n, raw).otherwise(bad_width)
        if isinstance(field.dataType, StringType):
            value = unescape(raw)
        elif isinstance(field.dataType, BooleanType):
            value = (
                F.when(raw == "t", F.lit(True))
                .when(raw == "f", F.lit(False))
                .otherwise(
                    F.raise_error(F.concat(F.lit(f"{table}.{field.name}: bad boolean "), raw))
                )
            )
        else:
            value = raw.cast(field.dataType)
        cols.append(
            F.when(raw == NULL, F.lit(None)).otherwise(value).cast(field.dataType).alias(field.name)
        )
    return lines.select(*cols)
