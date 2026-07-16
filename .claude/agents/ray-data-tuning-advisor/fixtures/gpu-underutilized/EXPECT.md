# Expected output for `gpu-underutilized`

The subagent output MUST:
- Fire **GPU underutilized** (S2): gpu_busy_fraction low (0.3 GPU used,
  pool_utilization 0.25) while a GPU op exists.
- Fire **One operator dominates wall time** (S3): the CPU preprocessing op
  MapBatches(decode_resize) dominates (~300s), starving the GPU op downstream.
- Root-cause the two together: GPU is starved by upstream CPU preprocessing.
- Recommend increasing CPU preprocessing parallelism/pods, and raising
  `batch_size`/`concurrency` on the GPU op; pair with KubeRay CPU worker-group
  scaling.
- Infer workload shape = batch inference (GPU).
