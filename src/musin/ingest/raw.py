"""Raw ingest: extracted dump tables -> typed Parquet (F1.3, FR-02).

    python -m musin.ingest.raw --config config/pipeline.yaml [--tables a,b]

Reads:  $MUSIN_DATA/dumps/<snapshot>/extracted/<archive>/mbdump/<table>  (from musin.ingest.extract)
Writes: $MUSIN_DATA/<paths.raw>/<table>/  Parquet, one dataset per table, overwritten
        $MUSIN_DATA/<paths.raw>/_manifest.json  row counts, sizes, runtimes

raw/ is a faithful copy of the dump (RULES.md §6): explicit types and nulls
only, no filtering, no renaming. Every line must have exactly the schema's
field count and every value must cast cleanly, otherwise the stage fails.
There is no sample mode here: sampling starts at the graph layer (F3.2), and
--tables restricts a development run to some tables.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime

from musin.common.config import ConfigError, load_config
from musin.common.spark import get_spark
from musin.ingest.extract import (
    archive_for,
    extracted_dir,
    read_schema_sequence,
    select_tables,
    table_path,
)
from musin.ingest.pgcopy import parse_copy
from musin.schemas.tables import SCHEMA_SEQUENCE, SCHEMAS, SOURCE_COMMIT

log = logging.getLogger("musin.ingest.raw")


def dir_size(path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--tables", help="comma-separated subset (default: all)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    tables = select_tables(args.tables)
    for archive in {archive_for(t) for t in tables}:
        seq = read_schema_sequence(extracted_dir(cfg, archive))
        if seq != SCHEMA_SEQUENCE:
            raise ConfigError(f"{archive}: SCHEMA_SEQUENCE {seq} != schemas' {SCHEMA_SEQUENCE}")
    for t in tables:
        if not table_path(cfg, t).is_file():
            raise FileNotFoundError(f"{table_path(cfg, t)} (run musin.ingest.extract first)")

    raw_root = cfg.data_path("raw")
    raw_root.mkdir(parents=True, exist_ok=True)
    spark = get_spark("musin-raw-ingest")
    # Malformed values must fail the stage, not silently become null.
    spark.conf.set("spark.sql.ansi.enabled", "true")

    start = time.monotonic()
    results: dict[str, dict] = {}
    for t in tables:
        t0 = time.monotonic()
        src, dest = table_path(cfg, t), raw_root / t
        lines = spark.read.option("lineSep", "\n").text(str(src))
        rows_in = lines.count()
        parse_copy(lines, SCHEMAS[t], t).write.mode("overwrite").parquet(str(dest))
        rows_out = spark.read.parquet(str(dest)).count()
        if rows_out != rows_in:
            raise RuntimeError(f"{t}: {rows_in} lines in, {rows_out} rows out")
        results[t] = {
            "rows": rows_out,
            "columns": len(SCHEMAS[t].fields),
            "tsv_bytes": src.stat().st_size,
            "parquet_bytes": dir_size(dest),
            "seconds": round(time.monotonic() - t0, 1),
        }
        log.info(
            "%-24s %12d rows  %8.1f MB -> %8.1f MB  %6.1fs",
            t,
            rows_out,
            results[t]["tsv_bytes"] / 1e6,
            results[t]["parquet_bytes"] / 1e6,
            results[t]["seconds"],
        )
    spark.stop()

    manifest_path = raw_root / "_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    manifest.update(
        {
            "snapshot": cfg.get("snapshot_date"),
            "schema_sequence": SCHEMA_SEQUENCE,
            "schema_source_commit": SOURCE_COMMIT,
            "tables": {**manifest.get("tables", {}), **results},
            "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    log.info(
        "done: %d tables, %d rows, in %.0fs",
        len(results),
        sum(r["rows"] for r in results.values()),
        time.monotonic() - start,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
