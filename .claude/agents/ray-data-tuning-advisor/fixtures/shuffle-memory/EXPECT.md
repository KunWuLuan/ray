# Expected output for `shuffle-memory`

The subagent output MUST:
- Fire **Shuffle/join memory pressure** (S9), citing the insufficient-memory and
  aggregators-not-ready warnings.
- Recommend reducing `max_hash_shuffle_aggregators` / partitions OR increasing
  cluster memory; pair with KubeRay pod-memory / worker-group scaling.
