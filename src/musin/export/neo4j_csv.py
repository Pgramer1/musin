"""Neo4j CSV export in neo4j-admin import format (F3.4, FR-14).

    python -m musin.export.neo4j_csv --config config/pipeline.yaml [--sample 0.05]
        [--variant <name>]

Reads:  <graph>/{nodes_artist,artist_genre,edges}  (F3.1) and <graph>/_checks.json (F3.3)
        <genres.mapping_file>  (the parent genres become Genre nodes)
Writes: $MUSIN_DATA/<paths.import>/<name>/  with <name> = "full" or "sample-f<fraction>",
                                            plus "-<variant>" for a variant graph (D-035)
          <file>.header.csv + <file>/part-*.csv   for every PRD §8.3 file
          _import_args.txt   the --nodes/--relationships arguments for neo4j-admin
          _export.json       source fingerprints, rows, null counts per file

Neo4j only sees $MUSIN_DATA/<paths.import> (mounted at /import), so sample
exports live there too, under their own name (D-031), not in the D-019 sample
tree. The export refuses a graph whose tables differ from its last passing
F3.3 check. Every file is read back with the schema its header declares and
must reproduce the source rows exactly (same content fingerprint).
"""

from __future__ import annotations

import argparse
import functools
import json
import logging
import operator
import re
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from musin.clean.genres import load_mapping
from musin.common.config import Config, ConfigError, load_config
from musin.common.layers import graph_name, layer_path
from musin.common.provenance import fingerprint, repo_commit
from musin.common.spark import get_spark
from musin.graph.build import EDGE_TYPES, GRAPH_TABLES
from musin.graph.checks import RESULT as CHECKS_FILE

log = logging.getLogger("musin.export.neo4j_csv")

# Where docker-compose.yml mounts $MUSIN_DATA/<paths.import> in the neo4j
# service: fixed wiring like NEO4J_URI, not a per-machine setting.
NEO4J_IMPORT_MOUNT = "/import"
EXPORT_FILE, ARGS_FILE = "_export.json", "_import_args.txt"
# Rows per CSV part: keeps the part count small and independent of Spark's
# shuffle partitions. Layout only; it never changes what is imported.
ROWS_PER_PART = 1_000_000

# PRD §8.3 headers (D-016: {id-type:long} on the Artist ID group).
HEADERS: dict[str, list[str]] = {
    "artists": [
        "artistId:ID(Artist){id-type:long}",
        "mbid",
        "name",
        "type",
        "areaName",
        "beginYear:int",
        "recordingCount:long",
        "primaryGenre",
        ":LABEL",
    ],
    "genres": ["name:ID(Genre)", ":LABEL"],
    "in_genre": [":START_ID(Artist)", ":END_ID(Genre)", "share:double", ":TYPE"],
    **{
        f"edges_{t.lower()}": [
            ":START_ID(Artist)",
            ":END_ID(Artist)",
            "weight:long",
            "firstYear:int",
            "lastYear:int",
            ":TYPE",
        ]
        for t in EDGE_TYPES
    },
}
NODE_FILES = ("artists", "genres")

# Explicit CSV dialect, shared by the writer and the read-back: RFC 4180
# quoting ("" inside quotes), no whitespace trimming (Spark trims by default
# when writing), null = empty unquoted field (Neo4j then omits the property).
CSV_OPTIONS = {
    "sep": ",",
    "quote": '"',
    "escape": '"',
    "escapeQuotes": "true",
    "nullValue": "",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "encoding": "UTF-8",
    "lineSep": "\n",
    "header": "false",
}

_ID_GROUP = re.compile(r"^(?:[^:]*):(ID|START_ID|END_ID)\((\w+)\)(\{id-type:long\})?$")
_TYPED = {"int": IntegerType(), "long": LongType(), "double": DoubleType()}


def field_type(field: str, long_id_groups: frozenset[str] = frozenset({"Artist"})) -> DataType:
    """Spark type of one import-header field, as neo4j-admin will parse it.

    `name:int` / `:long` / `:double` are typed; an ID, START_ID or END_ID is a
    long when its group has integer ids (Artist, D-016), else a string; any
    other field (plain property, :LABEL, :TYPE) is a string.
    """
    m = _ID_GROUP.match(field)
    if m:
        return LongType() if (m.group(3) or m.group(2) in long_id_groups) else StringType()
    if ":" in field and not field.startswith(":"):
        suffix = field.rsplit(":", 1)[1]
        if suffix not in _TYPED:
            raise ValueError(f"unsupported header type in {field!r}")
        return _TYPED[suffix]
    return StringType()


def header_schema(header: list[str]) -> StructType:
    """Read-back schema for a file with this header: positional columns c0..cN."""
    return StructType([StructField(f"c{i}", field_type(f), True) for i, f in enumerate(header)])


def blank_to_null(column: str) -> Column:
    """An empty string becomes null, i.e. a missing Neo4j property.

    Written as "" it is ambiguous: Spark reads it back as null, and the
    importer may store an empty string. The F3.1 graph has none (F3.4 doc §3).
    """
    return F.when(F.col(column) != "", F.col(column)).alias(column)


def artists_frame(nodes: DataFrame) -> DataFrame:
    """nodes_artist -> artists.csv columns (§8.3; artist_type -> type, genre_votes not exported)."""
    return nodes.select(
        F.col("artist_id").cast("long"),
        *[blank_to_null(c) for c in ("mbid", "name", "artist_type", "area_name")],
        F.col("begin_year").cast("int"),
        F.col("recording_count").cast("long"),
        blank_to_null("primary_genre"),
        F.lit("Artist").alias("label"),
    )


def genres_frame(spark: SparkSession, parents: set[str]) -> DataFrame:
    """One Genre node per parent genre of the mapping (D-024), with or without artists."""
    return spark.createDataFrame(
        [(p, "Genre") for p in sorted(parents)], "name string, label string"
    )


def in_genre_frame(artist_genre: DataFrame) -> DataFrame:
    """artist_genre -> in_genre.csv columns."""
    return artist_genre.select(
        F.col("artist_id").cast("long"),
        "parent_genre",
        F.col("share").cast("double"),
        F.lit("IN_GENRE").alias("type"),
    )


def edges_frame(edges: DataFrame, edge_type: str) -> DataFrame:
    """edges of one type -> edges_<type>.csv columns."""
    return edges.filter(F.col("edge_type") == edge_type).select(
        F.col("src_id").cast("long"),
        F.col("dst_id").cast("long"),
        F.col("weight").cast("long"),
        F.col("first_year").cast("int"),
        F.col("last_year").cast("int"),
        "edge_type",
    )


def newline_rows(df: DataFrame) -> int:
    """Rows with a CR or LF in any string column (they would need --multiline-fields)."""
    strings = [c for c, t in df.dtypes if t == "string"]
    if not strings:
        return 0
    any_break = functools.reduce(operator.or_, [F.col(c).rlike("[\\r\\n]") for c in strings])
    return df.filter(any_break).count()


def write_csv(df: DataFrame, header: list[str], target: Path, rows: int) -> list[Path]:
    """Write `<target>.header.csv` and `<target>/part-*.csv`; return the part files."""
    header_file = target.with_name(target.name + ".header.csv")
    header_file.write_text(",".join(header) + "\n", encoding="utf-8")
    parts = max(1, -(-rows // ROWS_PER_PART))
    df.coalesce(parts).write.mode("overwrite").options(**CSV_OPTIONS).csv(str(target))
    return sorted(target.glob("part-*.csv"))


def read_back(spark: SparkSession, target: Path, header: list[str]) -> DataFrame:
    """Parse written parts with the types the header declares (FAILFAST on bad rows)."""
    return (
        spark.read.options(**CSV_OPTIONS)
        .option("mode", "FAILFAST")
        .schema(header_schema(header))
        .csv(str(target / "part-*.csv"))
    )


def run(cfg: Config, sample: float | None) -> dict:
    """Export one graph folder; return the export record."""
    repo_root = cfg.source.resolve().parent.parent
    parents = {p for p in load_mapping(repo_root / cfg.get("genres.mapping_file")).values() if p}
    graph = layer_path(cfg, "graph", sample)
    name = graph_name(sample, cfg.variant)
    out = cfg.data_path("import") / name
    checks_file = graph / CHECKS_FILE
    if not checks_file.is_file():
        raise ConfigError(f"{checks_file} missing: run musin.graph.checks on this graph first")
    checked = json.loads(checks_file.read_text())["fingerprints"]

    spark = get_spark("musin-export-neo4j-csv")
    tables = {t: spark.read.parquet(str(graph / t)) for t in GRAPH_TABLES}
    current = {t: fingerprint(df)["fingerprint"] for t, df in tables.items()}
    if current != checked:
        spark.stop()
        raise ConfigError(
            f"{graph} differs from its last passing check ({checks_file}); "
            "re-run musin.graph.checks before exporting"
        )

    frames = {
        "artists": artists_frame(tables["nodes_artist"]),
        "genres": genres_frame(spark, parents),
        "in_genre": in_genre_frame(tables["artist_genre"]),
        **{f"edges_{t.lower()}": edges_frame(tables["edges"], t) for t in EDGE_TYPES},
    }
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    files: dict[str, dict] = {}
    args: list[str] = []
    problems: list[str] = []
    for stem, df in frames.items():
        header = HEADERS[stem]
        df = df.cache()
        source = fingerprint(df)
        if newline_rows(df):
            problems.append(f"{stem}: values with line breaks (need --multiline-fields)")
        parts = write_csv(df, header, out / stem, source["rows"])
        back = fingerprint(read_back(spark, out / stem, header))
        if back != source:
            problems.append(f"{stem}: read-back {back} != source {source}")
        nulls = df.agg(
            *[
                F.sum(F.col(c).isNull().cast("long")).alias(f)
                for c, f in zip(df.columns, header, strict=True)
            ]
        ).collect()[0]
        files[stem] = {
            "kind": "nodes" if stem in NODE_FILES else "relationships",
            "header": header,
            "rows": source["rows"],
            "fingerprint": source["fingerprint"],
            "parts": len(parts),
            "nulls": {f: nulls[f] for f in header if nulls[f]},
        }
        mounted = [f"{NEO4J_IMPORT_MOUNT}/{name}/{stem}.header.csv"] + [
            f"{NEO4J_IMPORT_MOUNT}/{name}/{stem}/{p.name}" for p in parts
        ]
        args.append(f"--{files[stem]['kind']}={','.join(mounted)}")
        df.unpersist()
        log.info("  %-24s %10d rows  %d part(s)", stem, source["rows"], len(parts))
    spark.stop()
    if problems:
        raise RuntimeError("export checks failed: " + "; ".join(problems))

    record = {
        "graph": name,
        "source": str(graph),
        "variant": cfg.variant,
        "variant_overrides": cfg.overrides,
        "exported_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo_commit": repo_commit(repo_root),
        "graph_fingerprints": checked,
        "files": files,
    }
    (out / ARGS_FILE).write_text("\n".join(args) + "\n", encoding="utf-8")
    (out / EXPORT_FILE).write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")
    log.info(
        "export '%s' -> %s; import with: bash neo4j/import_graph.sh %s --overwrite", name, out, name
    )
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--sample", type=float, help="export the sample graph of this fraction")
    parser.add_argument("--variant", help="export the variant graph defined under variants.<name>")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    run(load_config(args.config, args.variant), args.sample)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
