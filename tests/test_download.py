"""Tests for musin.ingest.download: checksum parsing, signature checking, hashing.

The fixtures are the real SHA256SUMS and SHA256SUMS.asc of the pinned
snapshot 20261007-002147, so the GPG test checks MusicBrainz's actual signature.
"""

import shutil
from pathlib import Path

import pytest

from musin.ingest.download import (
    VerificationError,
    expected_hashes,
    parse_sha256sums,
    sha256_file,
    signature_valid,
    snapshot_url,
    verify_signature,
)

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests" / "fixtures" / "dumps"
KEY = REPO / "config" / "musicbrainz-signing-key.asc"
FPR = "D5E63B4BDCCE195642948684B8FC2375C777580F"

needs_gpg = pytest.mark.skipif(shutil.which("gpg") is None, reason="gpg not installed")


def test_snapshot_url_handles_trailing_slash():
    assert snapshot_url("https://x/fullexport/", "20261007-002147", "a.tar.bz2") == (
        "https://x/fullexport/20261007-002147/a.tar.bz2"
    )
    assert snapshot_url("https://x/fullexport", "s", "f") == "https://x/fullexport/s/f"


def test_parse_sha256sums_binary_and_text_markers():
    text = "ABC123 *mbdump.tar.bz2\ndef456  mbdump-derived.tar.bz2\n\n"
    assert parse_sha256sums(text) == {
        "mbdump.tar.bz2": "abc123",
        "mbdump-derived.tar.bz2": "def456",
    }


def test_parse_real_sums_fixture():
    sums = parse_sha256sums((FIXTURES / "SHA256SUMS").read_text())
    assert len(sums) == 10
    assert sums["mbdump.tar.bz2"] == (
        "06c99bf3df27570c9de0e4130469d0452c6c49e74c799f9f817e2112d79763da"
    )


def test_expected_hashes_missing_file_fails():
    with pytest.raises(VerificationError, match="nope.tar.bz2"):
        expected_hashes({"a": "1"}, ["a", "nope.tar.bz2"])


def test_sha256_file_known_vectors(tmp_path):
    empty, abc = tmp_path / "empty", tmp_path / "abc"
    empty.write_bytes(b"")
    abc.write_bytes(b"abc")
    assert sha256_file(empty) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert sha256_file(abc) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_signature_valid_parses_status_lines():
    status = f"[GNUPG:] NEWSIG\n[GNUPG:] VALIDSIG {FPR} 2026-10-07 1 0 4 0 1 10 00 {FPR}\n"
    assert signature_valid(status, FPR)
    assert not signature_valid(status, "0" * 40)
    assert not signature_valid("[GNUPG:] BADSIG B8FC2375C777580F MusicBrainz", FPR)


@needs_gpg
def test_real_signature_verifies():
    verify_signature(FIXTURES / "SHA256SUMS", FIXTURES / "SHA256SUMS.asc", KEY, FPR)


@needs_gpg
def test_tampered_sums_fail(tmp_path):
    tampered = tmp_path / "SHA256SUMS"
    tampered.write_bytes((FIXTURES / "SHA256SUMS").read_bytes().replace(b"06c9", b"06c8"))
    with pytest.raises(VerificationError):
        verify_signature(tampered, FIXTURES / "SHA256SUMS.asc", KEY, FPR)


@needs_gpg
def test_wrong_pinned_fingerprint_fails():
    with pytest.raises(VerificationError):
        verify_signature(FIXTURES / "SHA256SUMS", FIXTURES / "SHA256SUMS.asc", KEY, "0" * 40)
