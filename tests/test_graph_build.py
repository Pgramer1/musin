"""Tests for musin.graph.build (F3.1) on tiny frames."""

import pytest

from musin.graph.build import (
    BAND_MEMBER,
    CO_CREDITED,
    EXCLUDED_CLASSICAL,
    EXCLUDED_TYPE,
    FEATURED,
    PERFORMED_FOR,
    PRODUCED_FOR,
    SAME_RECORDING_PAIR,
    SELF_LOOP,
    aggregate_by_decade,
    aggregate_edges,
    apply_node_rule,
    band_member_pairs,
    contribution_edges,
    credit_edges,
    credit_sizes,
    endpoints,
    excluded_artists,
    mark_drops,
    node_table,
    recording_counts,
    with_years,
)

CREDITS = "recording_id long, artist_id long, position int, role string"
INSTANCES = "recording_id long, src_id long, dst_id long, edge_type string"


@pytest.fixture
def credits(spark):
    rows = [
        # 1: "A feat. B & C"
        (1, 10, 0, "main"),
        (1, 11, 1, "featured"),
        (1, 12, 2, "featured"),
        # 2: "A & D"
        (2, 10, 0, "main"),
        (2, 13, 1, "main"),
        # 3: solo
        (3, 14, 0, "main"),
    ]
    return spark.createDataFrame(rows, CREDITS)


def edge_set(df):
    return sorted(
        (r["recording_id"], r["src_id"], r["dst_id"], r["edge_type"]) for r in df.collect()
    )


def test_credit_sizes(credits):
    assert {r["recording_id"]: r["n_artists"] for r in credit_sizes(credits).collect()} == {
        1: 3,
        2: 2,
        3: 1,
    }


def test_credit_edges_follow_prd_rules(credits):
    assert edge_set(credit_edges(credits)) == [
        (1, 10, 11, FEATURED),  # main -> featured
        (1, 10, 12, FEATURED),
        (1, 11, 12, CO_CREDITED),  # featured x featured, src < dst
        (2, 10, 13, CO_CREDITED),  # main x main, stored once
    ]


def test_contribution_edges_target_main_artists_only(spark, credits):
    contribs = spark.createDataFrame(
        [
            (1, 21, PERFORMED_FOR),  # recording 1 has one main (10), two featured
            (2, 20, PRODUCED_FOR),
            (2, 10, PRODUCED_FOR),  # a main artist who also produced: self-loop kept here
        ],
        "recording_id long, artist_id long, edge_type string",
    )
    assert edge_set(contribution_edges(contribs, credits)) == [
        (1, 21, 10, PERFORMED_FOR),
        (2, 10, 10, PRODUCED_FOR),
        (2, 10, 13, PRODUCED_FOR),
        (2, 20, 10, PRODUCED_FOR),
        (2, 20, 13, PRODUCED_FOR),
    ]


@pytest.fixture
def link_type(spark):
    rows = [
        (103, "member of band", "artist", "artist"),
        (102, "collaboration", "artist", "artist"),
        (999, "member of band", "artist", "label"),  # same name, other entity pair
    ]
    return spark.createDataFrame(
        rows, "id int, name string, entity_type0 string, entity_type1 string"
    )


def test_band_member_pairs_unordered_and_typed(spark, link_type):
    link = spark.createDataFrame([(1, 103), (2, 102), (3, 103), (4, 999)], "id int, link_type int")
    laa = spark.createDataFrame(
        [(1, 50, 40), (3, 41, 40), (1, 50, 40), (2, 60, 61), (4, 70, 71)],
        "link int, entity0 int, entity1 int",
    )
    pairs = sorted(tuple(r) for r in band_member_pairs(laa, link, link_type).collect())
    assert pairs == [(40, 41), (40, 50)]  # member -> group, normalised a < b, deduplicated


def test_band_member_pairs_fail_without_link_type(spark, link_type):
    link = spark.createDataFrame([(1, 102)], "id int, link_type int")
    laa = spark.createDataFrame([(1, 1, 2)], "link int, entity0 int, entity1 int")
    with pytest.raises(ValueError, match="member of band"):
        band_member_pairs(laa, link, link_type.filter("id != 103"))


@pytest.fixture
def artists(spark):
    rows = [(10, "Person"), (11, "Character"), (12, "Group"), (13, None), (14, "Character")]
    return spark.createDataFrame(rows, "artist_id long, artist_type string")


@pytest.fixture
def primary(spark):
    rows = [(12, "classical", 5), (13, "rock", 2), (14, "classical", 1)]
    return spark.createDataFrame(rows, "artist_id long, primary_genre string, genre_votes long")


def test_excluded_artists_by_type_and_classical(artists, primary):
    def reasons(df):
        return dict(sorted((r["artist_id"], r["reason"]) for r in df.collect()))

    assert reasons(excluded_artists(artists, primary, ["Character"], True)) == {
        11: EXCLUDED_TYPE,
        14: EXCLUDED_TYPE,
    }
    assert reasons(excluded_artists(artists, primary, ["Character"], False)) == {
        11: EXCLUDED_TYPE,
        12: EXCLUDED_CLASSICAL,
        14: EXCLUDED_TYPE,  # listed once, by type
    }
    assert reasons(excluded_artists(artists, primary, [], True)) == {}


def test_mark_drops_labels_first_reason(spark):
    instances = spark.createDataFrame(
        [
            (1, 10, 10, PRODUCED_FOR),  # self-loop
            (1, 11, 10, PERFORMED_FOR),  # excluded src
            (1, 10, 11, FEATURED),  # excluded dst
            (2, 41, 40, PERFORMED_FOR),  # member -> own group
            (2, 40, 41, CO_CREDITED),  # same pair, other direction
            (2, 40, 42, CO_CREDITED),  # kept
            (3, 50, 51, CO_CREDITED),  # billed pair ...
            (3, 51, 50, PERFORMED_FOR),  # ... so the main's own vocal credit adds nothing
            (3, 52, 50, PERFORMED_FOR),  # producer who also played: production wins
            (3, 52, 50, PRODUCED_FOR),
            (3, 53, 50, PERFORMED_FOR),  # contributor only: kept
            (4, 51, 50, PERFORMED_FOR),  # other recording: kept
        ],
        INSTANCES,
    )
    excluded = spark.createDataFrame([(11, EXCLUDED_TYPE)], "artist_id long, reason string")
    band = spark.createDataFrame([(40, 41)], "a long, b long")
    got = {
        (r["recording_id"], r["src_id"], r["dst_id"]): r["drop_reason"]
        for r in mark_drops(instances, excluded, band).filter("src_id != 52").collect()
    }
    assert got == {
        (1, 10, 10): SELF_LOOP,
        (1, 11, 10): EXCLUDED_TYPE,
        (1, 10, 11): EXCLUDED_TYPE,
        (2, 41, 40): BAND_MEMBER,
        (2, 40, 41): BAND_MEMBER,
        (2, 40, 42): None,
        (3, 50, 51): None,
        (3, 51, 50): SAME_RECORDING_PAIR,
        (3, 53, 50): None,
        (4, 51, 50): None,
    }
    by_type = {
        r["edge_type"]: r["drop_reason"]
        for r in mark_drops(instances, excluded, band)
        .filter("recording_id = 3 AND src_id = 52")
        .collect()
    }
    assert by_type == {PRODUCED_FOR: None, PERFORMED_FOR: SAME_RECORDING_PAIR}


@pytest.fixture
def dated(spark):
    instances = spark.createDataFrame(
        [
            (1, 10, 13, CO_CREDITED),
            (1, 10, 13, CO_CREDITED),  # duplicate instance counts once
            (2, 10, 13, CO_CREDITED),
            (3, 10, 13, CO_CREDITED),
            (4, 10, 13, CO_CREDITED),
            (4, 10, 13, FEATURED),  # same pair, other type
            (5, 20, 21, CO_CREDITED),
        ],
        INSTANCES,
    )
    years = spark.createDataFrame(
        [(1, 1994), (2, 1999), (3, 2003), (4, None), (5, None)],
        "recording_id long, first_year int",
    )
    return with_years(instances, years)


def test_aggregate_edges_weights_and_years(dated):
    got = {(r["src_id"], r["dst_id"], r["edge_type"]): r for r in aggregate_edges(dated).collect()}
    co = got[(10, 13, CO_CREDITED)]
    assert (co["weight"], co["first_year"], co["last_year"]) == (4, 1994, 2003)
    ft = got[(10, 13, FEATURED)]
    assert (ft["weight"], ft["first_year"], ft["last_year"]) == (1, None, None)
    assert got[(20, 21, CO_CREDITED)]["weight"] == 1
    assert dict(aggregate_edges(dated).dtypes)["weight"] == "bigint"


def test_aggregate_by_decade_and_invariant(dated):
    rows = aggregate_by_decade(dated).collect()
    got = {(r["src_id"], r["dst_id"], r["edge_type"], r["decade"]): r["weight"] for r in rows}
    assert got == {(10, 13, CO_CREDITED, 1990): 2, (10, 13, CO_CREDITED, 2000): 1}
    totals = {
        (r["src_id"], r["dst_id"], r["edge_type"]): r["weight"]
        for r in aggregate_edges(dated).collect()
    }
    by_edge: dict = {}
    for (s, d, t, _), w in got.items():
        by_edge[(s, d, t)] = by_edge.get((s, d, t), 0) + w
    assert all(w <= totals[k] for k, w in by_edge.items())  # PRD §8.1 invariant


def test_recording_counts_billed_or_contribution(spark, credits):
    contribs = spark.createDataFrame(
        [(2, 20), (3, 20), (2, 10)], "recording_id long, artist_id long"
    )
    got = {
        r["artist_id"]: r["recording_count"] for r in recording_counts(credits, contribs).collect()
    }
    assert got[10] == 2  # billed on 1 and 2; the contribution on 2 is not counted twice
    assert got[20] == 2  # producer only, never billed
    assert got[14] == 1


def test_node_rule_drops_edges_and_isolated_artists(spark):
    edges = spark.createDataFrame(
        [(10, 13, CO_CREDITED, 3), (10, 20, PRODUCED_FOR, 1), (20, 21, CO_CREDITED, 1)],
        "src_id long, dst_id long, edge_type string, weight long",
    )
    counts = spark.createDataFrame(
        [(10, 5), (13, 2), (20, 1), (21, 9)], "artist_id long, recording_count long"
    )
    kept = apply_node_rule(edges, counts, 2)
    assert kept.columns == edges.columns  # §8.1 column order survives the semi-joins
    assert sorted((r["src_id"], r["dst_id"]) for r in kept.collect()) == [(10, 13)]
    assert sorted(r["artist_id"] for r in endpoints(kept).collect()) == [10, 13]  # 21 isolated


def test_node_table_columns(spark, artists, primary):
    full = artists.selectExpr(
        "artist_id",
        "concat('mbid-', artist_id) as mbid",
        "concat('name-', artist_id) as name",
        "artist_type",
        "cast(null as string) as area_name",
        "cast(1990 as int) as begin_year",
    )
    counts = spark.createDataFrame([(10, 4), (13, 2)], "artist_id long, recording_count long")
    ids = spark.createDataFrame([(10,), (13,)], "artist_id long")
    out = node_table(ids, full, counts, primary)
    assert out.columns == [
        "artist_id",
        "mbid",
        "name",
        "artist_type",
        "area_name",
        "begin_year",
        "recording_count",
        "primary_genre",
        "genre_votes",
    ]
    got = {r["artist_id"]: r for r in out.collect()}
    assert got[10]["primary_genre"] is None and got[10]["genre_votes"] is None
    assert (got[13]["primary_genre"], got[13]["recording_count"]) == ("rock", 2)
