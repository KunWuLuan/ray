# Expected output for `backpressure`

The subagent output MUST:
- Fire **Submission backpressure / budget starvation** (S4).
- Cite the resource-starvation warning ("The job may hang forever unless the
  cluster scales up") AND the zero object-store/memory budget.
- Recommend scaling the cluster (add pods / raise maxReplicas) and/or raising
  resource_limits / relaxing exclude_resources; pair with KubeRay worker-group
  scaling.
- Confidence high (explicit starvation warning).
