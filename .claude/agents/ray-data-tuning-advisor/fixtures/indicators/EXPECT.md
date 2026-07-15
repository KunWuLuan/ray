# Expected output for `indicators`

The subagent output MUST:
- Report a non-trivial spill (spill_ratio > 0; "Spilled to disk: 4096MB" cited).
- Identify MapBatches(preprocess) as the dominant operator by wall-time share
  (80s of 100s total ≈ 80%).
- Note tasks-per-node skew on ReadParquet (90 vs 10 across 2 nodes).
- Cite exact numbers from the files; invent nothing.
