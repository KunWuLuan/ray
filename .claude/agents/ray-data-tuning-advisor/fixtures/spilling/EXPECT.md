# Expected output for `spilling`

The subagent output MUST:
- Fire the **Object-store spilling / OOM** symptom as a top finding.
- Cite BOTH the `Spilled to disk: 81920MB` figure AND the high-memory-detector
  warning as evidence.
- Recommend, in ranked order: reduce `target_max_block_size` and/or set
  `memory=` on the map op (config/pipeline), and raise object-store memory.
- Pair the object-store-memory recommendation with its KubeRay translation
  (bump worker-group pod `memory` request + `rayStartParams.object-store-memory`,
  or add a worker group / raise maxReplicas).
- Note the ~700MB/block profile as contributing.
