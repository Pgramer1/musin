"""Tests for musin.gds.setup (F3.5) without a Neo4j server."""

import pytest

from musin.common.config import ConfigError
from musin.gds.setup import (
    ALGORITHMS,
    CONSTRAINTS,
    INDEXES,
    PROJECTIONS,
    Session,
    native_projection,
    pair_expectations,
    parse_size,
    project,
)
from musin.graph.build import CO_CREDITED, EDGE_TYPES, FEATURED, PERFORMED_FOR, PRODUCED_FOR

EDGES = "src_id long, dst_id long, edge_type string, weight long, first_year int, last_year int"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("8G", 8 * 2**30),
        ("8g", 8 * 2**30),
        ("512m", 512 * 2**20),
        ("1024", 1024),
        ("2GB", 2 * 2**30),
    ],
)
def test_parse_size(text, expected):
    assert parse_size(text) == expected


def test_parse_size_rejects_garbage():
    with pytest.raises(ConfigError):
        parse_size("lots")


def test_projections_cover_prd():
    assert PROJECTIONS["collab_undirected"] == list(EDGE_TYPES)
    assert PROJECTIONS["credit_only"] == [CO_CREDITED, FEATURED]


def test_pair_expectations_merge_types_and_directions(spark):
    edges = spark.createDataFrame(
        [
            (1, 2, CO_CREDITED, 3, None, None),
            (2, 1, PERFORMED_FOR, 2, None, None),  # reverse direction, same pair
            (1, 3, FEATURED, 1, None, None),
            (4, 1, PRODUCED_FOR, 9, None, None),
        ],
        EDGES,
    )
    collab = pair_expectations(edges, PROJECTIONS["collab_undirected"], 2)
    assert collab == {"nodes": 4, "pairs": 3, "total_weight": 15, "top": [(1, 4, 9), (1, 2, 5)]}
    credit = pair_expectations(edges, PROJECTIONS["credit_only"], 5)
    assert credit == {"nodes": 3, "pairs": 2, "total_weight": 4, "top": [(1, 2, 3), (1, 3, 1)]}


def test_native_projection():
    assert native_projection([FEATURED]) == {
        FEATURED: {"type": FEATURED, "orientation": "UNDIRECTED", "properties": "weight"}
    }


def test_every_statement_the_code_uses_exists():
    names = set(Session(None).queries)
    used = {f"constraints.{n}" for n in (*CONSTRAINTS, *INDEXES)}
    used |= {
        "constraints.await_indexes",
        "constraints.show_constraints",
        "constraints.show_indexes",
        "projections.project_pairs",
        "projections.estimate_native",
        "projections.graph_info",
        "projections.weight_total",
        "projections.heaviest_pairs",
        "projections.list_graphs",
        "projections.drop",
    }
    used |= {f"estimates.{a}" for a in ALGORITHMS}
    assert used <= names, used - names


def test_project_rejects_unknown_name():
    with pytest.raises(ValueError, match="unknown projection"):
        project(None, "everything")
