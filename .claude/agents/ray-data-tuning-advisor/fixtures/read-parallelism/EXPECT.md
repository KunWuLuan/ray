# Expected output for `read-parallelism`

The subagent output MUST:
- Fire **Read over-parallelization** (S7), citing the ">4x CPU slots" warning.
- Recommend lowering `override_num_blocks` (or setting it to -1) / raising
  `read_op_min_num_blocks` appropriately.
- Note that many tiny blocks add task overhead.
