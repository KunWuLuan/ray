# Expected output for `hang`

The subagent output MUST:
- Fire **Hang / stall** (S11), citing BOTH the idle-detector ("no outputs for 90
  seconds") and hanging-detector ("stuck in scheduling for 300.00s") lines.
- Recommend checking the stuck task's stack trace / node health, and — if it's
  resource starvation — scaling the cluster; note a slow UDF as an alternative
  benign cause (confidence caveat).
