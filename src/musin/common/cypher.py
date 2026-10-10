"""Load named Cypher statements from the numbered files under neo4j/ (RULES §7).

A file holds one or more statements. Each starts with a `// @name <name>` line
and ends with a `;` at the end of a line. Other `//` comment lines are kept
(Cypher ignores them), so a query reads the same in the file and in the log.
"""

from __future__ import annotations

import re
from pathlib import Path

NAME_MARKER = re.compile(r"^\s*//\s*@name\s+(\S+)\s*$")


def parse_queries(text: str) -> dict[str, str]:
    """Split file text into {name: statement}, in file order, without the trailing `;`.

    Raises ValueError on a statement before the first marker, a duplicate name,
    an empty statement, or a statement that never ends with `;`.
    """
    queries: dict[str, str] = {}
    name: str | None = None
    body: list[str] = []

    def close() -> None:
        statement = "\n".join(body).strip()
        if not statement.endswith(";"):
            raise ValueError(f"statement '{name}' does not end with ';'")
        statement = statement[:-1].rstrip()
        if not any(not line.strip().startswith("//") for line in statement.splitlines()):
            raise ValueError(f"statement '{name}' is empty")
        queries[name] = statement

    for line in text.splitlines():
        marker = NAME_MARKER.match(line)
        if marker:
            if name is not None:
                close()
            name = marker.group(1)
            if name in queries:
                raise ValueError(f"duplicate statement name '{name}'")
            body = []
        elif name is not None:
            body.append(line)
        elif line.strip() and not line.strip().startswith("//"):
            raise ValueError(f"text before the first '// @name' marker: {line.strip()!r}")
    if name is not None:
        close()
    return queries


def load_queries(path: str | Path) -> dict[str, str]:
    """Read a .cypher file and return its named statements."""
    return parse_queries(Path(path).read_text(encoding="utf-8"))
