# Great Expectations suites

Expectation suites for the bronze and silver layers (guide: "Great Expectations
after bronze and silver, failures -> quarantine/").

- `suites/bronze_*.json` — shape/null/format checks on the raw landed tables
- `suites/silver_*.json` — the stronger contract: business keys **unique**
  (silver MERGE is the authoritative dedup), typed columns non-null, closed
  value sets

**Execution wiring** lands with Phase 9 (tests + CI): the suites run as GE
checkpoints against the Spark DataFrames inside the batch pipeline, and any
failing rows route to `s3a://lake/quarantine/` — same rule as the producer and
the batch validators (which already quarantine structurally invalid rows at
runtime, so quarantine coverage does not wait on GE).

Pinned at `great-expectations==0.18.8` (2023) in requirements.txt.

