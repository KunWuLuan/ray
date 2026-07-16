<!-- Project-specific Claude Code subagents. -->
<!-- See https://code.claude.com/docs/en/sub-agents -->
<!-- To create: .claude/agents/<name>/<name>.md -->
<!-- Example use cases: docs specialist, security reviewer -->

## Available agents

- **ray-data-tuning-advisor** (`ray-data-tuning-advisor/`) — diagnoses Ray Data
  execution bottlenecks from provided telemetry files (`ds.stats()` output,
  `ray-data.log`, optional Prometheus/Grafana export) and returns prioritized,
  KubeRay-aware tuning recommendations. See its `playbook.md` for the diagnostic
  knowledge base and `fixtures/` for worked examples.
