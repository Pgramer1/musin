"""Extract the needed dump tables from the verified archives (F1.3).

    python -m musin.ingest.extract --config config/pipeline.yaml [--tables a,b]

Reads:  $MUSIN_DATA/dumps/<snapshot>/<archive>.tar.bz2 (verified by F1.2)
Writes: $MUSIN_DATA/dumps/<snapshot>/extracted/<archive>/{SCHEMA_SEQUENCE,TIMESTAMP,mbdump/<table>}

Only the members for the tables in musin.schemas.tables are extracted, using
lbzip2 (multi-threaded). Each archive gets its own folder because both use
the same `mbdump/` prefix. Re-running overwrites the extracted files.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

from musin.common.config import Config, ConfigError, load_config
from musin.schemas.tables import DERIVED_TABLES, SCHEMA_SEQUENCE, SCHEMAS

log = logging.getLogger("musin.ingest.extract")

CORE_ARCHIVE = "mbdump.tar.bz2"
DERIVED_ARCHIVE = "mbdump-derived.tar.bz2"
METADATA = ("SCHEMA_SEQUENCE", "TIMESTAMP", "REPLICATION_SEQUENCE")


def archive_for(table: str) -> str:
    """Archive file that ships `table`."""
    return DERIVED_ARCHIVE if table in DERIVED_TABLES else CORE_ARCHIVE


def extracted_dir(cfg: Config, archive: str) -> Path:
    """Folder an archive is extracted into."""
    stem = archive.removesuffix(".tar.bz2")
    return cfg.data_path("dumps") / cfg.get("snapshot_date") / "extracted" / stem


def table_path(cfg: Config, table: str) -> Path:
    """Path of the extracted COPY file for `table`."""
    return extracted_dir(cfg, archive_for(table)) / "mbdump" / table


def select_tables(arg: str | None) -> list[str]:
    """Tables from --tables (comma-separated), or all known tables."""
    if not arg:
        return sorted(SCHEMAS)
    tables = [t.strip() for t in arg.split(",") if t.strip()]
    unknown = sorted(set(tables) - set(SCHEMAS))
    if unknown:
        raise ConfigError(f"unknown tables {unknown}; known: {sorted(SCHEMAS)}")
    return tables


def read_schema_sequence(folder: Path) -> int:
    return int((folder / "SCHEMA_SEQUENCE").read_text().strip())


def extract(cfg: Config, tables: list[str]) -> dict[str, int]:
    """Extract `tables`; return {table: bytes}. Fails on a schema-sequence mismatch."""
    snapshot_dir = cfg.data_path("dumps") / cfg.get("snapshot_date")
    sizes: dict[str, int] = {}
    for archive in (CORE_ARCHIVE, DERIVED_ARCHIVE):
        wanted = [t for t in tables if archive_for(t) == archive]
        if not wanted:
            continue
        if archive not in cfg.get("source.files"):
            raise ConfigError(f"{archive} is needed for {wanted} but not in source.files")
        src = snapshot_dir / archive
        dest = extracted_dir(cfg, archive)
        dest.mkdir(parents=True, exist_ok=True)
        members = [*METADATA, *(f"mbdump/{t}" for t in wanted)]
        t0 = time.monotonic()
        log.info("extracting %d tables from %s", len(wanted), archive)
        subprocess.run(
            ["tar", "-I", "lbzip2", "-xf", str(src), "-C", str(dest), *members], check=True
        )
        seq = read_schema_sequence(dest)
        if seq != SCHEMA_SEQUENCE:
            raise ConfigError(
                f"{archive} has SCHEMA_SEQUENCE {seq}, schemas were generated for "
                f"{SCHEMA_SEQUENCE}; regenerate musin.schemas.tables"
            )
        for t in wanted:
            sizes[t] = (dest / "mbdump" / t).stat().st_size
            log.info("  %-24s %14d bytes", t, sizes[t])
        log.info("%s done in %.0fs (schema sequence %d)", archive, time.monotonic() - t0, seq)
    return sizes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--tables", help="comma-separated subset (default: all)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    start = time.monotonic()
    sizes = extract(cfg, select_tables(args.tables))
    out = cfg.data_path("dumps") / cfg.get("snapshot_date") / "extracted" / "_extract.json"
    previous = json.loads(out.read_text()) if out.exists() else {}
    out.write_text(json.dumps({**previous, **sizes}, indent=2, sort_keys=True) + "\n")
    log.info(
        "done: %d tables, %.2f GB, in %.0fs",
        len(sizes),
        sum(sizes.values()) / 1e9,
        time.monotonic() - start,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
