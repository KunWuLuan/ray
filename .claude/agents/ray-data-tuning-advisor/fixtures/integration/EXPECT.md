# Expected output for `integration`

The subagent output MUST:
- Fire at least TWO symptoms: S1 (spilling/OOM — 40960MB spill + high-memory
  warning + ~650MB blocks) and S2 (GPU underutilized — 0.35 GPU usage, 0.3 pool
  util, tiny GPU-op wall-time share vs the big upstream read).
- Rank findings (the more impactful one first) with evidence lines for each.
- Emit all 5 report sections.
- In "What I couldn't assess" (or as a surfaced unmatched signal), mention the
  unrecognized `ray_data_future_unknown_signal` / `some_new_detector` line rather
  than silently dropping it (signal-reference appendix behavior).
- Provide a copy-pasteable quick-win config block.
- State that pod & autoscaling specifics need the RayCluster spec (not provided),
  while still giving the KubeRay translation for object-store memory + GPU pods.
- Invent no metric not present in the inputs.
