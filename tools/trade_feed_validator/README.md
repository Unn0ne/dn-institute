# Trade feed validator

This command validates trade events before they reach an analytics table. It
separates clean, unique trades from records that need investigation and produces a
machine-readable quality report. It requires Go 1.22 or newer and uses only the Go
standard library.

## Data-quality findings

| Source row | Event | Problem | Downstream impact | Handling |
|---|---|---|---|---|
| 4 | `evt_003` | Semantic duplicate of `evt_002`; ingestion metadata changed, but all trade fields match. | Adds 120,000 to volume and one trade to wallet `0xD4…`; can bias price-weighted metrics and overweight the wallet in clustering. | Keep the first valid occurrence; quarantine the replay. |
| 6 | `evt_005` | `block_time` is missing. | The trade cannot be assigned safely to a chain-time window, ordered, or used in latency calculations. | Quarantine it for backfill and replay. |
| 8 | `evt_007` | Semantic duplicate of `evt_006`, including identical ingestion metadata. | Adds 90,000 to volume and one trade to wallet `0xF6…`; inflates activity counts and clustering weight. | Keep the first valid occurrence; quarantine the replay. |
| 9 | `evt_008` | `ingested_at` is 10 minutes 10 seconds earlier than `block_time`. | Produces impossible negative ingestion latency and can break watermark or freshness logic. | Quarantine it until an authoritative source corrects a timestamp. |

Source-row numbers include the CSV header. A naive sum of all input amounts is
705,000; the validated analytics partition contains 375,000. Arrival order is not
treated as chain order: accepted rows are sorted by parsed `block_time`. No lateness
SLA was supplied, so a positive ingestion delay is not invented as an error. The
sample has no price field and therefore cannot produce a VWAP; duplicates would
distort its weights once price is included in a real feed.

The semantic duplicate fingerprint is `(tx_hash, block_time, wallet, side, amount)`.
It excludes `event_id` and `ingested_at`, which commonly change during replay, and
does not assume that `tx_hash` alone is unique: one blockchain transaction can
contain multiple trades. This fingerprint is a heuristic: distinct trades with
identical fields could be conflated. For EVM-like on-chain trade logs, a production
schema should include `chain_id` and `log_index`; `(chain_id, tx_hash, log_index)`
can identify the source log more reliably. The Ethereum execution API exposes
[`transactionHash` and `logIndex` in log records](https://ethereum.github.io/execution-apis/api/methods/eth_getLogs/).
Other chains and off-chain trades need their own authoritative event identity.

## Why `evt_005` goes to a dead-letter queue

Dropping it permanently would lose a potentially real 30,000-unit trade. Guessing
its time from `ingested_at` would silently put it in the wrong chain-time window. The
pipeline therefore excludes it from analytics but preserves the raw row, reason code,
and source row in `rejected_events.csv`. I would backfill `block_time` and replay the
event if an authoritative indexer or node can resolve `0xaa4` to a confirmed block. I
would drop it only if that lookup proves the transaction nonexistent or outside the
dataset's scope. If the product explicitly supported ingestion-time metrics, a
separate provisional table could retain it, clearly marked as incomplete.

## Validation behavior

The validator checks:

- CSV schema, duplicate headers, and malformed record widths;
- required fields and `null` values;
- UTC event and ingestion timestamps;
- one timestamp representation per accepted feed (time-only or RFC3339), with
  matching representations within each row;
- `BUY`/`SELL` side values and strictly positive decimal amounts;
- ingestion-time causality;
- duplicate `event_id` values and semantic trade replays.

Amounts use `math/big.Rat`, not `float64`, so large or fractional decimal values are
compared without rounding. Header order does not matter, a UTF-8 BOM is supported,
and extra named columns are preserved in both outputs. The three names used for
dead-letter metadata (`source_row`, `validation_codes`, and `validation_messages`)
are reserved. Invalid rows may carry multiple reason codes. Time-only values are
interpreted as one UTC day, as specified by the challenge; they are never compared
with dated RFC3339 values. Rows with mixed representations are quarantined with
`MIXED_TIMESTAMP_FORMAT`.
Schema or CSV syntax failures stop the run because row boundaries cannot be trusted;
data-quality failures are quarantined and do not stop other rows.

The implementation uses O(n) validation and de-duplication plus O(n log n) sorting
for deterministic chain-time output. A run writes all three artifacts to a fresh
generation directory before updating `LATEST`. Consumers should read `LATEST` once
per load and read every artifact from that directory; a failed run leaves the prior
generation selected. Individual files and the pointer are replaced with
`os.Rename`. On Unix, a same-filesystem rename gives atomic visibility, but
[Go does not guarantee atomicity on every platform](https://pkg.go.dev/os#Rename).

## Run the pipeline

From the repository root:

```bash
cd tools/trade_feed_validator
go mod download
go run .
```

There are no third-party dependencies. The default command reads `sample_feed.csv`
and writes `output/LATEST`, which contains the relative path of the latest complete
run directory, such as `runs/run-123456789`. That directory contains:

- `valid_trades.csv`: records safe for the analytics load;
- `rejected_events.csv`: dead-letter records with reason codes and messages;
- `validation_report.json`: counts and structured issue details.

For example, read `output/LATEST` once and use that same directory for all files:

```bash
run_dir="output/$(cat output/LATEST)"
ls "$run_dir"
```

To validate another feed or choose another destination:

```bash
go run . -input path/to/feed.csv -output-dir path/to/output
```

Use `-fail-on-rejections` when any quarantined record should produce exit status 1
in CI or orchestration. The default demonstration exits successfully because the
supplied sample is expected to contain rejected records. Fatal input, schema, or
output errors use exit status 2.

## Run the tests

From `tools/trade_feed_validator`:

```bash
go test ./... -v
go test -cover ./...
go test -race ./...
go vet ./...
```

## General practice (150 words or fewer)

Put an explicit data contract and quality gate between ingestion and analytics. The
gate should validate schema, types, domains, nullability, event-time causality, and a
stable idempotency key; route failures to a replayable dead-letter queue instead of
silently inserting them. Publish per-rule rejection counts, duplicate rates, missing
field rates, and ingestion-lag distributions, then alert on both fixed thresholds and
changes from a recent baseline. Enforce the same checks in unit tests with malformed
fixtures, in CI with contract tests, and in production on every batch or stream
window. Reconcile accepted counts and amounts against the source and periodically
replay quarantined records after authoritative backfills. Version rules and schemas so
producers and consumers can evolve safely, and assign owners and runbooks to every
alert. Back the idempotency key with a database constraint or durable state so
deduplication spans batches and restarts.
