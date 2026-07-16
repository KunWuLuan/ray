# Expected output for `trainer-starvation`

The subagent output MUST:
- Fire **Consumer/trainer starvation** (S5): iter_total_blocked (210s of 300s)
  and time_to_first_batch (45s) are large.
- Root-cause: the training loop waits on Ray Data; the pipeline can't produce
  batches fast enough.
- Recommend raising upstream `concurrency=`/prefetch and cluster capacity; pair
  with KubeRay worker-group scaling.
- Infer workload shape = training ingest.
