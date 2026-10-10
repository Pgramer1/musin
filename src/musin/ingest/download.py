"""Download and verify the pinned MusicBrainz full export (F1.2, FR-01).

    python -m musin.ingest.download --config config/pipeline.yaml
    python -m musin.ingest.download --config config/pipeline.yaml --verify-only
    python -m musin.ingest.download --config config/pipeline.yaml --show-latest

Output: $MUSIN_DATA/<paths.dumps>/<snapshot_date>/ with the files in
`source.files`, the signed SHA256SUMS(.asc), and manifest.json.

Trust chain: SHA256SUMS must carry a valid GPG signature from the key in
`source.signing_key_file` whose fingerprint equals
`source.signing_key_fingerprint`; every file must then match its SHA-256 in
SHA256SUMS. Anything else stops the run. Downloads resume from `<file>.part`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from http.client import HTTPException
from pathlib import Path

from musin.common.config import Config, ConfigError, load_config

log = logging.getLogger("musin.ingest.download")

SUMS = "SHA256SUMS"
SUMS_SIG = "SHA256SUMS.asc"
CHUNK = 8 * 1024 * 1024
USER_AGENT = "musin-coursework/0.1 (MusicBrainz full-export download)"
# Network robustness only; none of these can change a result.
MAX_ATTEMPTS = 8
TIMEOUT_S = 60
PROGRESS_EVERY_S = 30


class VerificationError(Exception):
    """A signature or checksum did not match."""


def snapshot_url(base_url: str, snapshot: str, name: str) -> str:
    """URL of `name` inside the snapshot directory."""
    return f"{base_url.rstrip('/')}/{snapshot}/{name}"


def parse_sha256sums(text: str) -> dict[str, str]:
    """Parse `sha256sum` output ("<hex> *<name>" or "<hex>  <name>") into {name: hex}."""
    sums: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        digest, name = line.split(maxsplit=1)
        sums[name.lstrip("*").strip()] = digest.lower()
    return sums


def expected_hashes(sums: dict[str, str], files: list[str]) -> dict[str, str]:
    """Pick the hashes for `files`; raise if SHA256SUMS does not list one."""
    missing = [f for f in files if f not in sums]
    if missing:
        raise VerificationError(f"{SUMS} has no entry for {missing}")
    return {f: sums[f] for f in files}


def signature_valid(gpg_status: str, fingerprint: str) -> bool:
    """True if gpg --status-fd output has a VALIDSIG line for `fingerprint`.

    VALIDSIG's first field is the signing (sub)key and its last field the
    primary key fingerprint, so either may carry the pinned fingerprint.
    """
    want = fingerprint.replace(" ", "").upper()
    for line in gpg_status.splitlines():
        parts = line.split()
        if parts[:2] == ["[GNUPG:]", "VALIDSIG"] and want in (parts[2], parts[-1]):
            return True
    return False


def sha256_file(path: Path) -> str:
    """Hex SHA-256 of a file, read in chunks."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def verify_signature(sums: Path, sig: Path, key_file: Path, fingerprint: str) -> None:
    """Check `sig` over `sums` with a throwaway keyring holding only `key_file`."""
    with tempfile.TemporaryDirectory() as home:
        base = ["gpg", "--batch", "--no-tty", "--homedir", home]
        subprocess.run([*base, "--import", str(key_file)], check=True, capture_output=True)
        result = subprocess.run(
            [*base, "--status-fd", "1", "--verify", str(sig), str(sums)],
            capture_output=True,
            text=True,
        )
    if result.returncode != 0 or not signature_valid(result.stdout, fingerprint):
        raise VerificationError(f"bad GPG signature on {sums}: {result.stderr.strip()}")


def _open(url: str, offset: int = 0, method: str = "GET"):
    headers = {"User-Agent": USER_AGENT}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=headers, method=method), timeout=TIMEOUT_S
    )


def remote_size(url: str) -> int:
    with _open(url, method="HEAD") as resp:
        return int(resp.headers["Content-Length"])


def fetch_small(url: str, dest: Path) -> None:
    """Download a small file in one go (checksum and signature files)."""
    with _open(url) as resp:
        dest.write_bytes(resp.read())


def fetch_large(url: str, dest: Path, size: int) -> None:
    """Download to `<dest>.part`, resuming after failures, then rename to `dest`."""
    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        offset = part.stat().st_size if part.exists() else 0
        if offset > size:
            part.unlink()
            offset = 0
        if offset == size:
            break
        try:
            with _open(url, offset) as resp:
                if offset and resp.status != 206:  # server ignored Range: start over
                    log.warning("  %s: server ignored Range, restarting", dest.name)
                    offset = 0
                elif offset:
                    log.info("  %s: resuming at byte %d of %d", dest.name, offset, size)
                mode = "ab" if offset else "wb"
                start, last_log, done = time.monotonic(), time.monotonic(), offset
                with part.open(mode) as fh:
                    while chunk := resp.read(CHUNK):
                        fh.write(chunk)
                        done += len(chunk)
                        if time.monotonic() - last_log >= PROGRESS_EVERY_S:
                            rate = (done - offset) / (time.monotonic() - start) / 1e6
                            log.info("  %s %.1f%% (%.1f MB/s)", dest.name, 100 * done / size, rate)
                            last_log = time.monotonic()
        except (urllib.error.URLError, HTTPException, TimeoutError, ConnectionError) as exc:
            wait = 10 * attempt
            log.warning("  %s attempt %d failed (%s); retry in %ds", dest.name, attempt, exc, wait)
            time.sleep(wait)
    if not part.exists() or part.stat().st_size != size:
        raise ConnectionError(f"{dest.name}: download incomplete after {MAX_ATTEMPTS} attempts")
    part.rename(dest)


def show_latest(cfg: Config) -> int:
    """Print the newest snapshot on the server and the sizes of `source.files`."""
    base = cfg.get("source.base_url")
    with _open(f"{base.rstrip('/')}/LATEST") as resp:
        latest = resp.read().decode().strip()
    print(f"latest snapshot: {latest} (pinned: {cfg.get('snapshot_date')})")
    for name in cfg.get("source.files"):
        print(f"  {name}: {remote_size(snapshot_url(base, latest, name)):,} bytes")
    return 0


def run(cfg: Config, verify_only: bool) -> int:
    snapshot = cfg.get("snapshot_date")
    if not snapshot:
        raise ConfigError("snapshot_date is null; pin one (see --show-latest)")
    base = cfg.get("source.base_url")
    files: list[str] = cfg.get("source.files")
    repo_root = cfg.source.resolve().parent.parent
    key_file = repo_root / cfg.get("source.signing_key_file")
    fingerprint = cfg.get("source.signing_key_fingerprint")

    dest = cfg.data_path("dumps") / snapshot
    dest.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    log.info("snapshot %s -> %s", snapshot, dest)

    for name in (SUMS, SUMS_SIG):
        if not (dest / name).exists():
            if verify_only:
                raise FileNotFoundError(dest / name)
            fetch_small(snapshot_url(base, snapshot, name), dest / name)
    verify_signature(dest / SUMS, dest / SUMS_SIG, key_file, fingerprint)
    log.info("%s signature ok (key %s)", SUMS, fingerprint)
    expected = expected_hashes(parse_sha256sums((dest / SUMS).read_text()), files)

    if not verify_only:
        sizes = {f: remote_size(snapshot_url(base, snapshot, f)) for f in files}
        partial = {f: (dest / f"{f}.part") for f in files}
        todo = sum(
            sizes[f] - (partial[f].stat().st_size if partial[f].exists() else 0)
            for f in files
            if not (dest / f).exists()
        )
        free = shutil.disk_usage(dest).free
        log.info("to download: %.2f GB, free: %.1f GB", todo / 1e9, free / 1e9)
        if todo > free:
            raise OSError(f"not enough disk: need {todo / 1e9:.1f} GB, have {free / 1e9:.1f} GB")
        for f in files:
            if (dest / f).exists():
                log.info("%s already present", f)
                continue
            t = time.monotonic()
            log.info("downloading %s (%.2f GB)", f, sizes[f] / 1e9)
            fetch_large(snapshot_url(base, snapshot, f), dest / f, sizes[f])
            log.info("%s downloaded in %.0fs", f, time.monotonic() - t)

    manifest_files = {}
    for f in files:
        path = dest / f
        if not path.exists():
            raise FileNotFoundError(path)
        t = time.monotonic()
        actual = sha256_file(path)
        if actual != expected[f]:
            raise VerificationError(f"{f}: sha256 {actual} != expected {expected[f]}")
        log.info("%s sha256 ok (%.0fs)", f, time.monotonic() - t)
        manifest_files[f] = {
            "url": snapshot_url(base, snapshot, f),
            "bytes": path.stat().st_size,
            "sha256": actual,
        }

    manifest = {
        "snapshot": snapshot,
        "files": manifest_files,
        "signature": {"file": SUMS_SIG, "key_fingerprint": fingerprint, "valid": True},
        "verified_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    total = sum(v["bytes"] for v in manifest_files.values())
    log.info(
        "done: %d files, %d bytes, verified, in %.0fs", len(files), total, time.monotonic() - start
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--verify-only", action="store_true", help="re-verify, no download")
    mode.add_argument("--show-latest", action="store_true", help="print newest server snapshot")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    if args.show_latest:
        return show_latest(cfg)
    return run(cfg, args.verify_only)


if __name__ == "__main__":
    sys.exit(main())
