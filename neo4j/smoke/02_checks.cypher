// F1.5 smoke test: what the import produced.

// @name versions
CALL dbms.components() YIELD name, versions, edition
RETURN name, versions[0] AS version, edition, gds.version() AS gds;

// @name label_counts
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n ORDER BY label;

// @name rel_type_counts
MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS n ORDER BY type;

// @name artist_property_types
MATCH (a:Artist)
RETURN a.artistId AS artistId,
       valueType(a.artistId) AS artistIdType,
       valueType(a.recordingCount) AS recordingCountType,
       valueType(a.beginYear) AS beginYearType,
       a.areaName IS NULL AS areaNameMissing,
       a.name AS name
ORDER BY artistId;

// @name edge_property_types
MATCH (s:Artist)-[r]->(t:Artist)
RETURN s.artistId AS src, t.artistId AS dst, type(r) AS type, r.weight AS weight,
       valueType(r.weight) AS weightType, r.firstYear AS firstYear, r.lastYear AS lastYear
ORDER BY src, dst, type;

// @name constraints
SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties
RETURN name, type, labelsOrTypes, properties ORDER BY name;
