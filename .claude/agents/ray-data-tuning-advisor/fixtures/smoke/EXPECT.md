# Expected output for `smoke`

The subagent output MUST:
- Contain all 5 report sections: Executive summary, Findings, Quick-win config, Pod & autoscaling plan, What I couldn't assess.
- Under "What I couldn't assess", note that no ray-data.log and no Prometheus export were provided.
- NOT invent any metric absent from stats.txt (e.g. no GPU numbers, no spill figures).
- Report no severe bottleneck (this is a trivial healthy job).
