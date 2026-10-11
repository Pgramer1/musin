// F3.4: read-only checks of a freshly imported graph (python -m musin.export.neo4j_check).
// Statements are named with "// @name <name>" and end with ";".
// No index exists yet (F3.5 creates them), so lookups use one label scan with IN.

// @name labels
CALL db.labels() YIELD label
RETURN collect(label) AS labels;

// @name node_counts
MATCH (n)
RETURN labels(n) AS labels, count(*) AS n;

// @name relationship_counts
MATCH ()-[r]->()
RETURN type(r) AS type, count(*) AS n;

// @name artist_report
// valueType() of a missing property is "NULL", so optional properties are
// type-checked only where present.
MATCH (a:Artist)
RETURN count(*) AS n,
       count(DISTINCT a.artistId) AS distinct_ids,
       sum(CASE WHEN valueType(a.artistId) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_artistId,
       sum(CASE WHEN valueType(a.recordingCount) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_recordingCount,
       sum(CASE WHEN a.beginYear IS NOT NULL AND valueType(a.beginYear) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_beginYear,
       count(*) - count(a.mbid) AS missing_mbid,
       count(*) - count(a.name) AS missing_name,
       count(*) - count(a.type) AS missing_type,
       count(*) - count(a.areaName) AS missing_areaName,
       count(*) - count(a.beginYear) AS missing_beginYear,
       count(*) - count(a.primaryGenre) AS missing_primaryGenre;

// @name artist_property_keys
MATCH (a:Artist)
UNWIND keys(a) AS key
RETURN key, count(*) AS n;

// @name genre_names
MATCH (g:Genre)
RETURN g.name AS name, keys(g) AS keys;

// @name relationship_report
MATCH ()-[r]->()
WHERE type(r) <> 'IN_GENRE'
RETURN type(r) AS type,
       count(*) AS n,
       sum(CASE WHEN valueType(r.weight) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_weight,
       sum(CASE WHEN r.firstYear IS NOT NULL AND valueType(r.firstYear) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_firstYear,
       sum(CASE WHEN r.lastYear IS NOT NULL AND valueType(r.lastYear) <> 'INTEGER NOT NULL' THEN 1 ELSE 0 END) AS bad_lastYear,
       count(*) - count(r.firstYear) AS missing_firstYear,
       count(*) - count(r.lastYear) AS missing_lastYear;

// @name in_genre_report
MATCH (:Artist)-[r:IN_GENRE]->(:Genre)
RETURN count(*) AS n,
       sum(CASE WHEN valueType(r.share) <> 'FLOAT NOT NULL' THEN 1 ELSE 0 END) AS bad_share;

// @name artists_by_id
MATCH (a:Artist)
WHERE a.artistId IN $ids
RETURN a.artistId AS artist_id, a.mbid AS mbid, a.name AS name, a.type AS artist_type,
       a.areaName AS area_name, a.beginYear AS begin_year,
       a.recordingCount AS recording_count, a.primaryGenre AS primary_genre;

// @name edges_from
MATCH (s:Artist)-[r]->(d:Artist)
WHERE s.artistId IN $ids
RETURN s.artistId AS src_id, d.artistId AS dst_id, type(r) AS edge_type,
       r.weight AS weight, r.firstYear AS first_year, r.lastYear AS last_year;
