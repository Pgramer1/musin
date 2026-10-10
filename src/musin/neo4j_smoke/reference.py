"""Independent reference answers for the toy graph (F1.5).

Plain-Python versions of what GDS computes, small enough to check by hand, so
the smoke test compares GDS against a second implementation instead of
against numbers typed into the test. Only for toy-sized graphs.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict, deque
from pathlib import Path

Pair = tuple[int, int]

# ":START_ID(Artist)" -> ("", "START_ID", "Artist"); "weight:long" -> ("weight", "long", None);
# "artistId:ID(Artist){id-type:long}" -> ("artistId", "ID", "Artist").
HEADER_FIELD = re.compile(
    r"^(?P<name>[^:]*)(?::(?P<kind>[A-Za-z_]+))?(?:\((?P<group>[^)]*)\))?(?:\{(?P<opts>.*)\})?$"
)
INT_KINDS = {"int", "long", "short", "byte"}
FLOAT_KINDS = {"double", "float"}
ID_KINDS = {"id", "start_id", "end_id"}


def read_import_csv(path: str | Path, int_id_groups: frozenset[str]) -> list[dict]:
    """Read a neo4j-admin import CSV (header in the first line) into typed dicts.

    Keys are the property name, or the field kind for unnamed fields
    (`START_ID`, `END_ID`, `TYPE`, `LABEL`). ID fields of an ID group in
    `int_id_groups` (the groups declared `{id-type:long}` in the node file)
    and int/long fields become int, double fields float. Empty fields become
    None, as the importer leaves them unset.
    """
    with Path(path).open(encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        fields = []
        for raw in header:
            m = HEADER_FIELD.match(raw)
            if m is None:
                raise ValueError(f"unparseable header field {raw!r} in {path}")
            kind = (m.group("kind") or "string").lower()
            numeric_id = kind in ID_KINDS and m.group("group") in int_id_groups
            fields.append((m.group("name") or m.group("kind"), kind, numeric_id))
        rows = []
        for values in reader:
            row = {}
            for (name, kind, numeric_id), value in zip(fields, values, strict=True):
                if value == "":
                    row[name] = None
                elif kind in INT_KINDS or numeric_id:
                    row[name] = int(value)
                elif kind in FLOAT_KINDS:
                    row[name] = float(value)
                else:
                    row[name] = value
            rows.append(row)
    return rows


def merged_pairs(edges: list[tuple[int, int, int]]) -> dict[Pair, float]:
    """COLLAB projection: (src, dst, weight) of any type/direction -> {(lo, hi): summed weight}."""
    merged: dict[Pair, float] = defaultdict(float)
    for src, dst, weight in edges:
        if src == dst:
            raise ValueError(f"self-loop {src}")
        merged[(min(src, dst), max(src, dst))] += weight
    return dict(merged)


def neighbours(pairs: dict[Pair, float]) -> dict[int, dict[int, float]]:
    """Undirected adjacency {node: {neighbour: weight}} from merged pairs."""
    adj: dict[int, dict[int, float]] = defaultdict(dict)
    for (a, b), w in pairs.items():
        adj[a][b] = w
        adj[b][a] = w
    return dict(adj)


def weighted_degree(pairs: dict[Pair, float]) -> dict[int, float]:
    """Sum of incident edge weights per node."""
    return {node: sum(nbrs.values()) for node, nbrs in neighbours(pairs).items()}


def pagerank(pairs: dict[Pair, float], damping: float, tolerance: float) -> dict[int, float]:
    """Weighted PageRank in the GDS form, iterated to convergence.

    PR(v) = (1 - d) + d * sum_u PR(u) * w(u, v) / W(u), with W(u) the weighted
    degree of u. Scores are not normalised to sum to 1, as in GDS. Every node
    must have an edge (true for the graph layer, PRD §8.3).
    """
    adj = neighbours(pairs)
    strength = {u: sum(nbrs.values()) for u, nbrs in adj.items()}
    score = {u: 1 - damping for u in adj}
    while True:
        new = {
            v: (1 - damping) + damping * sum(score[u] * w / strength[u] for u, w in nbrs.items())
            for v, nbrs in adj.items()
        }
        if max(abs(new[v] - score[v]) for v in adj) < tolerance:
            return new
        score = new


def betweenness(multiplicity: dict[Pair, int]) -> dict[int, float]:
    """Exact unweighted betweenness (Brandes) of an undirected (multi)graph.

    `multiplicity` maps each (lo, hi) pair to its number of parallel edges.
    Parallel edges multiply shortest-path counts, which is how they change the
    scores. Each unordered (s, t) pair counts once.
    """
    adj: dict[int, dict[int, int]] = defaultdict(dict)
    for (a, b), m in multiplicity.items():
        adj[a][b] = m
        adj[b][a] = m
    score = {v: 0.0 for v in adj}
    for s in adj:
        order, preds = [], defaultdict(list)
        sigma = defaultdict(int, {s: 1})
        dist = {s: 0}
        queue = deque([s])
        while queue:
            v = queue.popleft()
            order.append(v)
            for w, m in adj[v].items():
                if w not in dist:
                    dist[w] = dist[v] + 1
                    queue.append(w)
                if dist[w] == dist[v] + 1:
                    sigma[w] += sigma[v] * m
                    preds[w].append((v, m))
        delta = defaultdict(float)
        for w in reversed(order):
            for v, m in preds[w]:
                delta[v] += sigma[v] * m / sigma[w] * (1 + delta[w])
            if w != s:
                score[w] += delta[w]
    return {v: x / 2 for v, x in score.items()}


def modularity(pairs: dict[Pair, float], community: dict[int, int]) -> float:
    """Weighted Newman modularity Q of a partition of an undirected graph."""
    total = sum(pairs.values())  # m
    strength = weighted_degree(pairs)
    inside: dict[int, float] = defaultdict(float)
    degree_sum: dict[int, float] = defaultdict(float)
    for (a, b), w in pairs.items():
        if community[a] == community[b]:
            inside[community[a]] += w
    for node, k in strength.items():
        degree_sum[community[node]] += k
    return sum(inside[c] / total - (degree_sum[c] / (2 * total)) ** 2 for c in degree_sum)
