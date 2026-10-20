"""Tests for the F6.2 demo file and musin.demo_check's parameter parsing."""

from pathlib import Path

import pytest

from musin.common.cypher import load_queries
from musin.demo_check import DEMO_FILE, demo_params

DEMO = Path(__file__).resolve().parent.parent / "neo4j" / DEMO_FILE


def test_demo_params():
    text = "// :param name => 'Ry Cooder';\n//   :param limit => 40;\nMATCH (n) RETURN n;"
    assert demo_params(text) == {"name": "Ry Cooder", "limit": 40}
    with pytest.raises(ValueError):
        demo_params("MATCH (n) RETURN n;")


def test_demo_file_parses_and_uses_only_declared_params():
    queries = load_queries(DEMO)
    params = demo_params(DEMO.read_text(encoding="utf-8"))
    assert list(queries)[0] == "demo_overview" and len(queries) == 8
    assert all(name.startswith("demo_") for name in queries)
    for name, query in queries.items():
        code = "\n".join(line for line in query.splitlines() if not line.strip().startswith("//"))
        used = {p for p in params if f"${p}" in code}
        undeclared = {w for w in code.split("$")[1:] if w.split()[0].strip("}),.;") not in params}
        assert not undeclared, f"{name} uses undeclared parameters"
        assert used <= set(params)


def test_demo_is_read_only():
    for name, query in load_queries(DEMO).items():
        upper = query.upper()
        for word in ("CREATE ", "MERGE ", "SET ", "DELETE ", "REMOVE ", "DETACH "):
            assert word not in upper, f"{name} writes ({word.strip()})"
