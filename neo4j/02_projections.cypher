// F3.5: GDS projections for P4 (PRD §5, D-017).
// Built, checked and dropped by `python -m musin.gds.setup`; P4 code builds
// them with musin.gds.setup.project() and drops them when done (RULES §7).
//   collab_undirected  all four edge types (COLLAB, PRD §5)
//   credit_only        CO_CREDITED + FEATURED (RQ4 sensitivity, PRD §5)

// @name project_pairs
// One undirected relationship per artist pair (D-017): endpoints are put in
// artistId order, so a FEATURED a->b and a PERFORMED_FOR b->a meet on the same
// pair, and weights of all $types are summed. With D-028 the summed weight is
// the number of recordings the pair shares. Only artists with at least one
// relationship of $types are projected (for COLLAB that is every artist, §8.3).
MATCH (s:Artist)-[r]->(t:Artist)
WHERE type(r) IN $types
WITH CASE WHEN s.artistId < t.artistId THEN s ELSE t END AS a,
     CASE WHEN s.artistId < t.artistId THEN t ELSE s END AS b,
     r.weight AS weight
WITH a, b, sum(weight) AS w
WITH gds.graph.project(
       $name, a, b,
       {relationshipProperties: {weight: toFloat(w)}},
       {undirectedRelationshipTypes: ['*']}
     ) AS g
RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS relationships,
       g.projectMillis AS project_millis;

// @name estimate_native
// Heap guard before projecting: a Cypher-aggregation projection has no
// .estimate, so estimate the native projection of the same types, which keeps
// parallel relationships and is therefore an upper bound for the merged graph.
CALL gds.graph.project.estimate('Artist', $relationshipProjection)
YIELD requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount
RETURN requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount;

// @name graph_info
CALL gds.graph.list($name)
YIELD graphName, nodeCount, relationshipCount, memoryUsage, sizeInBytes
RETURN graphName, nodeCount, relationshipCount, memoryUsage, sizeInBytes;

// @name weight_total
// Each undirected pair is stored in both directions.
CALL gds.graph.relationshipProperty.stream($name, 'weight')
YIELD sourceNodeId, targetNodeId, propertyValue
RETURN count(*) AS relationships, sum(propertyValue) AS total;

// @name heaviest_pairs
CALL gds.graph.relationshipProperty.stream($name, 'weight')
YIELD sourceNodeId, targetNodeId, propertyValue
WITH gds.util.asNode(sourceNodeId).artistId AS a, gds.util.asNode(targetNodeId).artistId AS b,
     propertyValue AS w
WHERE a < b
RETURN a, b, w
ORDER BY w DESC, a, b
LIMIT $k;

// @name list_graphs
CALL gds.graph.list() YIELD graphName
RETURN collect(graphName) AS graphs;

// @name drop
CALL gds.graph.drop($name, false) YIELD graphName
RETURN graphName AS dropped;
