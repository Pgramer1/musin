# Toy graph for the Neo4j + GDS smoke test (F1.5)

Hand-made, 12 artists, in the PRD §8.3 import-CSV format. Two clusters
(`rock` 101–105, `hip hop` 107–112) joined only through artist 106, so the
expected answers are known: 106 has the highest betweenness, and no Louvain
community mixes the two clusters. (Louvain finds 4 communities, modularity
0.476, which beats the plain 2-way genre split at 0.461.)

Deliberate edge cases:

- `artistId:ID(Artist){id-type:long}`: the per-group override that makes
  `artistId` an integer property while `Genre` keeps string IDs (PRD §8.3,
  D-016). Without it, `artistId` is imported as a string.
- 108–109 has both `CO_CREDITED` and `FEATURED` (109 → 108), and 101–103 has
  `CO_CREDITED` 101 → 103 plus `PERFORMED_FOR` 103 → 101: the COLLAB
  projection must merge these into one undirected pair each, summing weights
  (D-017).
- Artist 105 has empty `areaName` and `beginYear`, edge 101–105 empty years:
  empty fields must become missing properties, not `""` or `0`.
- Names with a comma and with escaped quotes test CSV quoting.
- Genre `electronic` has one artist share only (112): a genre that no
  community matches.
