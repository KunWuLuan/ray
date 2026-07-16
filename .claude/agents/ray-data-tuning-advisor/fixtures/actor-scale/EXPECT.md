# Expected output for `actor-scale`

The subagent output MUST:
- Fire **Actor pool won't scale** (S6), citing the "will not allow it to scale
  up" warning verbatim.
- Explain the max_tasks_in_flight_per_actor / max_concurrency ceiling.
- Recommend raising max_tasks_in_flight_per_actor and/or lowering the upscaling
  threshold, and confirming worker-group maxReplicas can host more actors.
