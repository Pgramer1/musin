// F1.5 smoke test: GDS projections of the toy graph.

// @name drop_if_exists
CALL gds.graph.list() YIELD graphName
WITH graphName WHERE graphName IN ['collab_undirected', 'collab_native']
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN dropped;

// @name collab_undirected
// COLLAB projection (PRD glossary): every edge type merged into one undirected,
// weighted relationship per artist pair. The pair is put in artistId order first,
// so a FEATURED a->b and a PERFORMED_FOR b->a land on the same pair, and their
// weights are summed. Every graph-layer artist has at least one edge (PRD §8.3),
// so a plain MATCH loses no nodes.
MATCH (s:Artist)-[r:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR]->(t:Artist)
WITH CASE WHEN s.artistId < t.artistId THEN s ELSE t END AS a,
     CASE WHEN s.artistId < t.artistId THEN t ELSE s END AS b,
     r.weight AS weight
WITH a, b, sum(weight) AS w
WITH gds.graph.project(
       'collab_undirected', a, b,
       {relationshipProperties: {weight: toFloat(w)}},
       {undirectedRelationshipTypes: ['*']}
     ) AS g
RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS relationships;

// @name collab_native
// Native projection with all four types as separate undirected relationship
// types. Kept only to show why the merged projection is needed: parallel
// relationships between the same pair are counted separately.
CALL gds.graph.project(
  'collab_native',
  'Artist',
  {
    CO_CREDITED:   {orientation: 'UNDIRECTED', properties: 'weight'},
    FEATURED:      {orientation: 'UNDIRECTED', properties: 'weight'},
    PRODUCED_FOR:  {orientation: 'UNDIRECTED', properties: 'weight'},
    PERFORMED_FOR: {orientation: 'UNDIRECTED', properties: 'weight'}
  }
) YIELD graphName, nodeCount, relationshipCount
RETURN graphName AS graph, nodeCount AS nodes, relationshipCount AS relationships;

// @name drop_all
CALL gds.graph.list() YIELD graphName
WITH graphName WHERE graphName IN ['collab_undirected', 'collab_native']
CALL gds.graph.drop(graphName) YIELD graphName AS dropped
RETURN dropped;

// @name list_graphs
CALL gds.graph.list() YIELD graphName RETURN count(*) AS graphs;
