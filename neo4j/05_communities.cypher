// F4.2: Louvain communities on collab_undirected (RQ2, PRD §5.5, D-034).
// Run by `python -m musin.analytics.communities`. Every Louvain run is a
// mutate run under its own property name ($property), so the result of record
// (single-threaded, reproducible) and the stability runs (several threads) can
// be streamed side by side. Weighted degree for the example artists comes
// from neo4j/04_centrality.cypher (`centrality.degree`).
// Louvain uses GDS defaults for maxLevels (10), maxIterations (10) and
// tolerance (0.0001); the run records them from `configuration`.

// @name estimate_louvain
CALL gds.louvain.mutate.estimate($graph, {
  relationshipWeightProperty: 'weight', mutateProperty: 'louvain', concurrency: $concurrency
})
YIELD requiredMemory, bytesMax
RETURN requiredMemory, bytesMax;

// @name louvain
CALL gds.louvain.mutate($graph, {
  relationshipWeightProperty: 'weight', mutateProperty: $property, concurrency: $concurrency
})
YIELD modularity, communityCount, ranLevels, computeMillis, nodePropertiesWritten, configuration
RETURN modularity, communityCount, ranLevels, computeMillis, nodePropertiesWritten,
       configuration.maxLevels AS maxLevels, configuration.maxIterations AS maxIterations,
       configuration.tolerance AS tolerance;

// @name wcc
// Connected components (D-034): almost every small community is a whole small
// component, so genre agreement is also reported inside the largest component,
// where Louvain actually has to divide a connected network.
CALL gds.wcc.mutate($graph, {mutateProperty: 'component', concurrency: $concurrency})
YIELD componentCount, computeMillis
RETURN componentCount, computeMillis;

// @name genres
// The parent genres (F2.5), so the composition table has one column for each,
// including genres that no top community contains.
MATCH (g:Genre)
RETURN g.name AS genre
ORDER BY genre;

// @name write_louvain
// PRD §8.3: the community of record becomes :Artist.louvain. The value is the
// community's size rank (1 = largest), not GDS's internal id, so it matches
// the report tables. Written in batches of $rows.
UNWIND $rows AS r
MATCH (a:Artist {artistId: r.artistId})
SET a.louvain = r.community
RETURN count(a) AS written;

// @name count_written
MATCH (a:Artist)
RETURN count(a) AS artists, count(a.louvain) AS louvain;
