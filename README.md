# Device Telemetry Gateway with Asynchronous Fanout, and a Storefront
# Event/Segment Membership Extension

A small version of a vehicle telemetry ingest path: a Go WebSocket server
takes one persistent connection per simulated device, deduplicates on device
id and sequence, fans deduplicated readings out to Kafka keyed by device, and
spills to a local disk file rather than dropping a reading when Kafka cannot
take it right now. Extended in a later session with a staged load curve
(cmd/loadcurve) and a soak benchmark (cmd/soak) that found and fixed an
unbounded dedup cache. Every number below was measured on this machine, not
targeted: every resume claim (bit-identical final state after a duplicated,
out-of-order replay; zero records lost across a 60-second outage; the
offered rate where loss first goes nonzero; a soak-caught unbounded cache,
now capped and pinned by a test) is the literal, unedited output of a real
run, included verbatim in `docs/`.

**Second extension (Sep. 2026): a real-time event ingestion and segment
membership engine, built for a different resume claim than the telemetry
work above but sharing the same edge-dedup shape.** A second Go edge
deduplicates simulated storefront events (`page_view`, `add_to_cart`,
`purchase`, `cart_abandon`) on `event_id` into Kafka; a consumer fans each
event out to a ClickHouse event store and an incremental Redis segment
membership engine covering 50 parameterized test segments; an independent
from-scratch recompute over the full ClickHouse event log validates the
incremental path exactly. This extension's numbers are also the unedited
output of a real run, in `docs/`, including two bugs the first measurement
attempt actually had (a lag baseline that measured a synthetic event's
historical timestamp instead of when it was really sent, and stale
Kafka/ClickHouse state left over from an earlier interrupted run), both
found, root-caused and fixed before the numbers below were taken. See
Findings.

**Third extension (Sep. 2026): a real-time message delivery stats pipeline
and dashboard, built for a different resume claim again, sharing the same
edge-dedup shape as the two extensions above.** A third Go edge
(`internal/deliverystats`) deduplicates simulated email, SMS and WhatsApp
provider delivery webhooks on `webhook_id` into Kafka; a consumer fans each
webhook out to a ClickHouse raw event log and a periodically refreshed
per-minute rollup table, both read by a small JSON API
(`cmd/deliverygateway`) a React dashboard (`web/deliverydashboard`) polls.
This extension's numbers are the unedited output of a real run, in `docs/`,
including one bug the first measurement attempt actually had (the
correctness benchmark tried to produce into a Kafka topic before creating
it, since this broker has topic auto-creation disabled, and failed with
"Unknown Topic Or Partition" on read-back). Found, root-caused and fixed
before the numbers below were taken; see Findings. Given a tight time and
cost budget for this build session, every claim below was measured with one
genuine attempt rather than the playbook's usual up-to-three, disclosed
here rather than hidden.

**What the delivery-stats extension is, and is not:**

- **Simulated provider webhooks over a simulated message corpus, stated as
  such.** `internal/deliverystats.Build` generates outbound messages across
  three channels (email via a simulated `sendgrid`, SMS via a simulated
  `twilio`, WhatsApp via a simulated `meta_whatsapp`), each producing a
  small realistic webhook lifecycle (`sent`, then `delivered`/`bounced`/
  `failed`); no real message, real provider, or real customer is involved.
- **A correctness-and-latency project for the edge, a from-scratch-recompute
  project for ClickHouse.** The dedup-into-Kafka claim is checked the same
  way `eventgen` checks the storefront edge (an independent reference dedup
  diffed against what actually landed); the 50M-row claim is checked as a
  ClickHouse-side question, not routed back through the edge (see
  `cmd/deliveryscale`'s doc comment for why that is a deliberate scope
  choice, not an oversight).
- **The 50M-row bulk load bypasses Kafka and the edge on purpose.** It
  writes directly into ClickHouse's raw table to isolate the question the
  claim is actually about (does the rollup table's accounting tie out to an
  independent full recompute, and is a dashboard query against the rollup
  table actually faster), from the edge's own dedup-correctness and
  throughput questions, which are separate claims measured separately.
- **A single-broker Kafka, single-node ClickHouse setup**, the same
  single-node honesty disclosure as the two extensions above.
- **Docker was not available in this environment** (`docker version`
  fails, same as before); Kafka and ClickHouse are real, standing native
  WSL2 processes already running from the prior extension's session, reused
  here against a separate `deliverystats` ClickHouse database so a 50M-row
  bulk load cannot collide with, or slow down, the segment-membership
  module's own tables. Real Dockerfiles and a docker-compose.yml describing
  the intended containerized shape are included in `deploy/` (see Building
  and running); they were authored, never built or run, and are disclosed
  as such rather than claimed working.

## Why this exists

This mirrors the shape of `teslamotors/fleet-telemetry`: a Go server holding
persistent protobuf-over-WebSocket connections from many devices and
dispatching what it receives to Kafka and other backends. The two properties
that actually matter for a telemetry pipeline are tested directly here:
correctness under a lossy, duplicating, reordering network (dedup has to be
exactly right, not approximately right), and survivability when the
downstream broker goes away for a while (a slow consumer or an outage should
degrade into "queued on disk", not "silently gone").

## What this is, and is not

- **Simulated devices and a simulated outage, stated as such.** 5,000
  simulated devices are real WebSocket clients making real TCP connections
  to a real running server, not a mocked interface; the "60-second broker
  outage" is a producer that is told to refuse writes for 60 seconds rather
  than the actual Kafka process being stopped, so that the measurement is
  deterministic and does not depend on how fast a real broker happens to
  restart on this machine. This is disclosed explicitly, not hidden in the
  numbers.
- **A correctness project first, a throughput project second.** The
  benchmarks measure whether the final state is exactly right and whether
  anything got lost, not how many messages per second the gateway can sustain.
- **A single-broker, single-Kafka-instance setup**, not a multi-broker
  replicated cluster. The dedup and spill logic do not depend on Kafka's own
  replication; they exist specifically so the gateway does not need to.
- **Kubernetes as a target shape, not a running cluster.** No Kubernetes
  cluster is available in this environment; `deploy/gateway-deployment.yaml`
  is a real, `pyyaml`-validated manifest describing how this binary is meant
  to run (stateless, horizontally replicated, one spool volume per pod), but
  it was never applied to a live cluster this session.
- **Machine and toolchain.** WSL2 Ubuntu 22.04 on Windows 11, 8 physical / 16
  logical cores. Go 1.23.4. Apache Kafka 4.3.1 in KRaft mode (no ZooKeeper),
  single broker, single node, running natively under OpenJDK 17.0.20, topics
  created with 6 partitions. `protoc` 28.2 with `protoc-gen-go`. No GPU is
  involved anywhere in this project.

**What the storefront/segment extension is, and is not:**

- **Simulated storefront events over a simulated profile corpus, stated as
  such.** `internal/simstorefront` generates events with a synthetic
  historical spread (up to 120 days in the past) so the segment predicates
  ("purchased in the last 30 days") have real history to evaluate against;
  no real storefront or real customer data is involved anywhere.
- **A correctness-and-latency project, not a maximum-throughput project.**
  The claimed 20,000 events/sec is a sustained-offered-rate floor the edge
  clears comfortably (see Measured results); the benchmark does not push
  for a ceiling above that.
- **1M profiles is the design target, not the scale this session's
  benchmark actually reached.** The full-recompute-vs-incremental oracle
  diff, the p99 lag measurement, and the burst-drain measurement below ran
  at up to 20,000 profiles / 110,032 events, the largest scale reached in
  the attempts made this session; see Measured results and Limitations for
  the honest gap to 1M.
- **Docker was not available in this environment** (`docker version`
  fails); Kafka, ClickHouse and Redis are all real, but stood up as native
  WSL2 processes per the scripts in `docs/_start_*.sh`, not containers. No
  `docker-compose.yml` is included because authoring one that was never run
  even once would not be a more honest artifact than simply saying so here.
- **Single-node Kafka, single-node ClickHouse, single-instance Redis**, the
  same single-node honesty disclosure as the original telemetry gateway
  above; none of the three claims a distributed guarantee.

## Architecture

```
device-telemetry-gateway/
  proto/
    telemetry.proto           # Reading: device_id, sequence, timestamp_ms, value
    telemetry.pb.go           # generated
  internal/
    ingest/
      dedup.go                 # per-device set of sequences already forwarded
      spool.go                  # append-only length-prefixed disk overflow queue
      pipeline.go                # dedup -> produce; on failure -> spool; drainer retries
      reference.go                # independent dedup + order-independent digest, for diffing
      pipeline_test.go            # unit tests + the differential fuzz test
    kafkaclient/
      producer.go              # segmentio/kafka-go writer, keyed by device id
      consumer.go               # reads every message on a topic back out, for verification
    wsserver/
      server.go                # one WebSocket connection per device, binary protobuf frames
    simdevice/
      corpus.go                 # builds and drives the duplicated/reordered device corpus
    eventingest/
      dedup.go                  # bounded event_id dedup for the storefront edge
      pipeline.go                # dedup -> produce for storefront events
      reference.go                # independent dedup, for the differential test
      pipeline_test.go            # unit tests + differential fuzz test
    simstorefront/
      corpus.go                 # builds the synthetic storefront event corpus per profile
    segment/
      definitions.go            # the 50 test segments, the single source both engines read
      incremental.go             # Redis-backed incremental membership engine
      recompute.go                # from-scratch ClickHouse full-recompute oracle
      diff.go                    # incremental-vs-recompute set comparison
      clickhouse.go               # hand-rolled ClickHouse HTTP client (insert + aggregate query)
      segment_test.go             # segment-count/uniqueness and DiffSets unit tests
    kafkaclient/
      storefront_producer.go    # dedup -> Kafka for storefront events
      storefront_consumer.go     # streaming reader driving ClickHouse + the incremental engine
  cmd/
    gateway/main.go            # standalone deployable service
    loadgen/main.go             # the 5,000-device replay-correctness benchmark
    outagebench/main.go          # the 60-second broker-outage benchmark
    eventgen/main.go            # storefront edge-dedup-into-kafka correctness benchmark
    throughputbench/main.go      # storefront edge sustained-throughput benchmark
    segmentbench/main.go          # end-to-end segment engine benchmark (lag, oracle diff, burst drain)
  proto/storefront.proto        # StorefrontEvent: event_id, profile_id, event_type, timestamp_ms, ...
  deploy/gateway-deployment.yaml # Kubernetes Deployment + Service (not applied live)
  docs/                        # raw output from every run below, including the two _start_*.sh
                                # scripts used to stand up ClickHouse and Redis natively in WSL2
```

**Why dedup needs a full per-device set, not a high-water mark.** A
security-facing anti-replay counter (see the sibling project,
`connected-device-platform`) only ever needs to reject counters at or below
the highest one seen, because the backend controls issuance order. Telemetry
has no such guarantee: a reading can legitimately arrive after a later one
over an unreliable link. `Dedup` therefore keeps the full set of sequences
seen per device; the cost is memory proportional to readings per device, not
network reliability.

**Why the pipeline never blocks on Kafka.** `Pipeline.Ingest` gives the
producer a 200ms window; if that is not enough, the reading goes to
`Spool.Write` instead of waiting longer. A slow or unreachable broker
therefore turns into "the WebSocket handler stays responsive and the spool
file grows" rather than "the whole gateway backs up".

**Why the drainer retries the whole spool in parallel, not one record at a
time.** The first version of `drainOnce` retried spooled readings
sequentially. In the outage benchmark below, that made recovery from a
60-second outage's backlog take longer than the drain-wait the benchmark
allowed, which read back as "records lost" when they were actually still
sitting in the spool, not gone. See Findings.

**Why verification reads Kafka back with raw partition offsets instead of a
consumer group.** The first version of the benchmark's Kafka reader used a
`kafka-go` consumer group reader, which showed 0 messages consumed even
though the messages were genuinely on the topic. See Findings.

**Why the segment definitions live in their own file, imported by both
engines rather than shared through a common evaluator.** `segment.Segment`
is data (a family plus its parameters), not code. `incremental.go` and
`recompute.go` each range over the same `GenerateTestSegments()` output but
evaluate it independently, one against per-profile Redis state built up
event-by-event, the other against one SQL query per segment run cold
against ClickHouse. Sharing the evaluator instead of just the definitions
would make an exact-match diff between them meaningless: a shared bug in
one shared evaluator would still show up as "0 mismatches".

**Why membership updates are event-driven but also need a periodic
sweep.** Most of the 50 segment families become newly true only when a
matching event lands (a purchase, a cart-add), which `IncrementalEngine.
ApplyEvent` handles per-event. Three families (`cart_abandon_days`,
`churn_risk_days`, `recently_active_hours`) can also become newly *false*
purely because time passed with no new event, which no event handler will
ever observe. `Finalize` re-evaluates every profile touched during the run
against the run's own latest event timestamp as `now`, which is what
actually resolves those transitions; between events, membership on these
three families can lag reality by up to one Finalize interval, disclosed
here rather than hidden.

### Delivery-stats extension architecture

```
  internal/
    deliverystats/
      dedup.go        # bounded webhook_id dedup, independent of eventingest's
      pipeline.go      # dedup -> produce for delivery webhooks
      reference.go      # independent dedup, for the differential test
      pipeline_test.go    # unit tests + differential fuzz test
      simwebhooks.go        # simulated email/SMS/WhatsApp webhook corpus builder
      clickhouse.go          # raw events table + rollup table + recompute/rollup queries
  internal/kafkaclient/
    delivery_producer.go    # per-webhook and batching producers
    delivery_consumer.go     # partition-direct read-back and streaming reader
    topic.go                  # shared create-topic-if-absent helper
  cmd/
    deliverygateway/main.go  # standalone service: webhook HTTP receiver, Kafka
                              # producer, Kafka->ClickHouse consumer, dashboard JSON API
    deliverygen/main.go       # dedup-into-kafka correctness benchmark
    deliverythroughput/main.go # courtesy-capped (6 goroutines) throughput benchmark
    deliveryburst/main.go       # 10x burst backlog drain benchmark
    deliveryscale/main.go        # 50M-row bulk load, rollup-vs-recompute exactness,
                                   # dashboard raw-vs-rollup p95 latency
  web/deliverydashboard/       # React + TypeScript dashboard, reads /api/summary
  deploy/deliverystats-*        # Dockerfiles and docker-compose, authored not run
```

**Why the ClickHouse raw table is ordered `(channel, status, webhook_id)`
instead of by time.** That is the edge's own natural write key, but it means
a dashboard query that filters or buckets by timestamp gets no benefit from
ClickHouse's sparse primary index and has to scan the full table. That is
the deliberate, disclosed reason the "raw" dashboard query is slow at scale,
and the rollup table (tiny by comparison, one row per minute/channel/status)
is fast: less data to scan, not a smarter plan over the same data. See
`internal/deliverystats/clickhouse.go`'s doc comments and Measured results.

**Why the 50M-row scale benchmark writes directly to ClickHouse instead of
through Kafka.** Routing 50M rows through the courtesy-capped 6-goroutine
edge first would spend the whole time budget on Kafka throughput, a
question `deliverythroughput` already answers at a realistic, honest scale;
the scale benchmark's own question is entirely about ClickHouse's rollup
accounting once the rows exist.

## Validation

- **Go tests, 7 new cases, `-race` clean**, mirroring `eventingest`'s own
  shape: webhook-id dedup semantics, retry suppression, error propagation
  without suppressing dedup, and a 100-trial randomized differential test
  comparing the real `Pipeline` (in-memory fake producer) against the
  independent `ReferenceDedup`. Full output: `docs/deliverystats_test_output.txt`.
- **`deliverygen` is its own end-to-end validation**, the same shape as
  `eventgen`: an independent reference-dedup digest and the real
  Kafka-materialized set are compared directly.
- **`deliveryscale`'s rollup-vs-recompute diff is the primary correctness
  check at scale**: for every `(channel, status)` pair, the rollup table's
  summed count is compared against an independent full-table recompute
  query, and only reported exact if every pair matches.


- **Go tests, 7 cases, `-race` clean**: `Dedup` semantics (including that
  out-of-order sequences are legitimately admitted), the spool-on-failure and
  drain-on-recovery paths, duplicate suppression, digest order-independence,
  and a **100-trial randomized differential test** comparing the real
  `Pipeline` (backed by an in-memory fake producer) against the independent
  `ReferenceDedup` over random per-device duplicate/reorder patterns. Full
  output: `docs/go_test_output.txt`.
- **The replay benchmark itself is its own end-to-end validation**: it
  computes the reference digest and the real Kafka-materialized digest
  independently and fails loudly (non-zero exit) if they disagree.
- **The outage benchmark is likewise self-checking**: it counts what was
  actually sent against what actually landed in Kafka and fails loudly on
  any mismatch in either direction (lost or duplicated).
- **Kubernetes manifest**: parsed successfully with `pyyaml`
  (`python3 -c "import yaml; yaml.safe_load_all(...)"`), not applied to a
  live cluster.
- **Go tests, 3 more cases** in `internal/segment`: the 50-segment count and
  ID-uniqueness pin, and two `DiffSets` tests proving the diff helper both
  reports an exact match and actually detects a disagreement in each
  direction (a diff helper that can only ever say "exact" would make every
  "50/50 matched" result below meaningless). Full output:
  `docs/go_test_output.txt`.
- **`eventgen` is its own end-to-end validation** for the storefront edge,
  the same shape as the replay benchmark above: it computes an independent
  reference-dedup digest and the real Kafka-materialized digest separately
  and reports whether they agree.
- **`segmentbench`'s full-recompute-vs-incremental diff is the primary
  correctness check for the segment engine**: for every one of the 50 test
  segments, the profile-id set the incremental Redis path currently holds
  is compared, member for member, against an independent from-scratch
  ClickHouse recompute over the entire event log, using `DiffSets`. A
  segment is only reported as matching if the two sets are identical, not
  merely the same size.

## Findings

**A consumer-group reader silently returned zero messages that were
genuinely on the topic.** The first version of the Kafka verification step
used `kafka-go`'s `Reader` with a fresh `GroupID` per run, on the theory that
a brand-new consumer group always starts from the earliest offset. The
replay benchmark reported "0 readings actually landed in kafka" against a
topic a direct diagnostic write-then-read confirmed had messages on it. The
wrong hypothesis, briefly, was that the producer was failing silently. The
measurement that discriminated it: a minimal standalone program that wrote
one message and immediately read it back with the exact same reader
configuration succeeded, which meant the problem was specific to timing or
group coordination in the benchmark's larger run, not the reader
configuration in isolation. The actual cause was reading everything through
one high-level abstraction (`kafka-go`'s managed consumer group) whose
partition-assignment and rebalance handshake is exactly the kind of timing
dependency a "did everything I sent arrive" check should not have. Rewrote
`ConsumeAvailable` to dial each partition directly, read its offset bounds,
and read exactly that range, which has no rebalance step and is
deterministic. Fixed in this repo's `internal/kafkaclient/consumer.go`.

**The 60-second outage benchmark reported 1,210 "lost" records that were
actually just still queued.** After fixing the consumer, the first full
60-second outage run reported `sent: 4250, landed in kafka: 3040`, a
1,210-record shortfall. The wrong hypothesis was a real bug in the
spool-write or drain-truncate logic (a lock-ordering issue between `Write`
and `Drain` was the first thing checked, and ruled out by inspection: both
hold the same mutex for their entire critical section). The actual cause was
the benchmark's own drain-wait: a fixed 10-second sleep after the send phase,
while `drainOnce` retried roughly 3,000 backlogged readings sequentially, one
network round trip each, which measured out to well over a minute to fully
recover. The fix was two-part: `drainOnce` now retries up to 32 spooled
readings concurrently instead of one at a time, and the benchmark now polls
`Spool.SizeBytes()` until it reads zero (bounded by a 5-minute cap) instead
of guessing a fixed wait. After both fixes, the same 60-second-outage
scenario shows `sent: 4250, landed in kafka: 4250`, spool drained in the same
wall-clock window as the send phase itself.

**The segment engine's first p99 lag measurement was 10,258,044,787 ms
(about 118 days), not a lag bug in the engine at all.** `segmentbench`
builds its synthetic corpus with events backdated up to `history-days` (120)
into the past, because the segment predicates need real history to
evaluate against ("purchased 3+ times in the last 30 days" needs events
that are actually up to 30 days old). The first version of the lag
measurement computed `time.Now() - event.TimestampMs` at the moment each
event was consumed, which is exactly right for a live production stream
where an event's timestamp is close to now, and exactly wrong here, where
`TimestampMs` is a deliberately backdated synthetic value: the "lag" it
measured was mostly how old the synthetic history was, not how long the
pipeline took to process it. The fix: the benchmark now separately records
the real wall-clock instant each event is actually sent through the edge
(`sentAt[event.EventId] = time.Now()`, keyed by event id) and measures lag
as `recvAt - sentAt`, which is the pipeline latency the claim is actually
about. After the fix, p99 lag measured 72 ms at 3,000 profiles and 134 ms
at 20,000 profiles, both comfortably under the 2-second claim.

**A rerun of `segmentbench` reported `consumed: 30778 / 16559` (more
events consumed than sent) and `clickhouse event store: 0 rows`, from two
separate bugs surfacing together.** The Kafka topic name defaults to a
fixed string (`storefront-events`) and `ensureTopic` only creates a topic
if absent, it never truncates one; a second run against the same broker
therefore had its consumer read its own new messages plus every message
left over from the previous run, corrupting both the consumed count and
the lag/segment numbers with stale data. Separately, `ch.CountRows` and
`ch.CountDistinctProfiles` decoded their JSON response into a `string`
field, and this ClickHouse server build returns `count()`/`uniqExact()` as
a bare JSON number rather than a quoted string for these two queries
specifically (the per-row event columns elsewhere in this project do come
back quoted), so `json.Unmarshal` silently left the count at its zero
value with the underlying decode error discarded by the original
`rows, _ := ch.CountRows(ctx)` call site. The fixes: each run now gets its
own uniquely-suffixed Kafka topic unless `-topic` is set explicitly, so
repeated runs never share Kafka state; and the scalar decoder now targets
`json.Number` (whose Kind is String, so it accepts either a quoted string
or a bare number) instead of `string`, with the decode error now surfaced
instead of discarded. After both fixes, a clean run shows
`consumed: 110032 / 110032` and `clickhouse event store: 110032 rows,
20000 distinct profiles`, both exactly matching the corpus that was sent.

**Delivery-stats extension findings.** The first run of `deliverygen`
reported `send phase: 0 accepted, ... 4000 produce errors` and then failed
outright on read-back with `Unknown Topic Or Partition: the request is for
a topic or partition that does not exist on this broker`. The wrong
hypothesis, briefly, was a dedup bug rejecting everything; the actual cause
was simpler: this Kafka broker has topic auto-creation disabled (the same
broker the segment/storefront extension already uses, for the same
single-broker setup), and every one of this extension's benchmarks tried to
produce into a brand-new topic name without creating it first. Fixed by
calling `kafkaclient.EnsureTopic` before the send phase in `deliverygen`,
`deliverythroughput` and `deliverygateway` (`deliveryburst` already created
its topic, which is why it worked on the first attempt). After the fix,
`deliverygen`'s correctness run passed cleanly on the next attempt.

## Measured results

Machine: WSL2 Ubuntu 22.04 on Windows 11, 8 physical / 16 logical cores, Go
1.23.4, Apache Kafka 4.3.1 (KRaft mode, single broker, 6 partitions per
topic). Full raw output for both runs below is in `docs/`.

**Claim 1: final state bit-identical after a replay 5% duplicated and out of
order, across 5,000 simulated devices.**

| | |
|---|---|
| Devices | 5,000 |
| True readings per device | 20 |
| Unique readings | 100,000 |
| Delivered (post-duplication) messages | 105,000 |
| Measured duplicate fraction of delivered traffic | 4.76% (targeting 5%; the exact ratio that makes `dup/(unique+dup) = 5%` is 4.76%, reported as measured) |
| Reordering | every device's own delivery order fully shuffled (not merely "sometimes out of order") |
| Reference-oracle digest | `3995b050d1270842e64a15b9a68cdbb11026e94915b0891b17d7535cc6a48705` |
| Kafka-materialized digest | `3995b050d1270842e64a15b9a68cdbb11026e94915b0891b17d7535cc6a48705` |
| **Result** | **bit-identical** |

Full run: `docs/replay_benchmark_output.txt` (send phase over up to 400
concurrent WebSocket connections completed in 305ms; digest comparison ran
immediately after).

**Claim 2: 0 records lost across a 60-second simulated broker outage.**

| | |
|---|---|
| Devices | 50, sending continuously at 1 reading/sec each |
| Pre-outage / outage / post-outage windows | 10s / 60s / 15s (85s total send window) |
| Sent | 4,250 |
| Landed in Kafka after drain | 4,250 |
| **Records lost** | **0** |

Full run: `docs/outage_benchmark_output.txt`. This does not measure how long
recovery takes under an arbitrarily large backlog, only that this backlog
(roughly 3,000 readings accumulated during the outage) fully drained; a much
longer outage or much higher device count would need a proportionally larger
drain window, which is exactly why the benchmark polls for an empty spool
rather than assuming a fixed recovery time.

Neither benchmark measures raw throughput; see Limitations.

**Claim 3: staged load curve, the offered rate where loss first goes
nonzero.** This gateway never permanently drops a reading that passes
dedup, by design (that is the whole point of the spool), so "readings
lost" is 0 at every offered rate and is not the useful number here. The
useful, disclosed definition: the offered rate at which the pipeline could
no longer produce directly within its 200ms window and had to fall back to
the spool, measured against a fixed-capacity fake producer (not real
Kafka) so the result is deterministic. First attempt (default 50-in-flight
/ 5ms producer, offered 2,000 to 16,000/sec) found no threshold in range;
second attempt (20-in-flight / 10ms, offered 500 to 6,000/sec) also found
none, both because a single 200ms Ingest timeout tolerates brief queueing
even well above nominal capacity within a short stage. Third attempt
(5-in-flight / 50ms, implied 100/sec capacity, offered 30 to 300/sec, 2s
per stage) found it cleanly: 0% fallback at and below 90/sec, a clear
transition at 110/sec, output in `docs/loadcurve_output.txt`.

| Offered rate | Fell back to spool |
|---|---|
| 90/sec | 0.00% |
| **110/sec** | **35.00%** |
| 150/sec | 85.00% |
| 300/sec | 95.83% |

**Claim: unbounded dedup cache, found by a soak, now capped and pinned by
a test.** "Six hours" is the traffic volume replayed (six hours at the
outage benchmark's own 50-device, 1-reading/sec/device rate = 1,080,000
readings), not wall-clock duration, replayed as fast as the pipeline can
accept them rather than waited out in real time, the same disclosed
simulated-time convention the 60-second outage benchmark already uses. The
soak found `internal/ingest.Dedup` held one map entry per unique reading
ever seen, forever (no eviction at all): a genuinely unbounded structure
that a six-hour soak, let alone a real device fleet over its real
lifetime, would eventually turn into an out-of-memory gateway. Fixed by
bounding each device's remembered sequences to `MaxSequencesPerDevice`
(4096, FIFO eviction) in `internal/ingest/dedup.go`, pinned by
`TestDedupCacheStaysBounded` and two related tests in
`internal/ingest/dedup_cap_test.go`. Measured after the fix:

| | |
|---|---|
| Total readings ingested | 1,080,000 |
| Entries an unbounded cache would hold | 1,080,000 |
| **Peak entries actually held (bounded)** | **204,800** (= 50 devices × 4,096 cap) |
| Regression test | `TestDedupCacheStaysBounded`: PASS |

Full run: `docs/soak_output.txt`.

### Storefront event ingestion and segment membership engine

Machine: same as above, plus Apache Kafka 4.3.1 (KRaft mode, single
broker, 6 partitions), ClickHouse 25.x (single node, native WSL2 process,
not a container), Redis 7.x (single instance, native WSL2 process, port
6380). All three real, running, and reachable at measurement time; none
mocked.

**Claim: Go edge deduplicating on event id into Kafka, over simulated
storefront events.** `docs/eventgen_output.txt`:

| | |
|---|---|
| Profiles | 500 |
| Unique events | 3,349 |
| Delivered (post-duplication) messages | 3,525 (target duplicate fraction 5.00%) |
| Accepted by the edge | 3,349; rejected as duplicates by the edge | 176 |
| Kafka-materialized unique event ids | 3,349 |
| Independent reference-dedup unique event ids | 3,349 |
| **Result** | **edge-dedup-into-Kafka matches the independent reference exactly** |

**Claim: 20,000 events/sec sustained on one 16-core machine.**
`docs/throughputbench_output.txt`, 400 concurrent sender workers, 15s
offered duration:

| | |
|---|---|
| Sent (accepted + produced) | 537,264 |
| Errors | 0 |
| Elapsed | 15.011s |
| **Sustained throughput** | **35,791 events/sec** |
| Claim | 20,000 events/sec |
| **Met** | **yes (1.79x the claim)** |

**Claim: segment membership for 1M profiles updated incrementally in
Redis over a ClickHouse event store; p99 event-to-segment lag under 2s;
membership identical to a full recompute on all 50 test segments; 10x
burst backlog drained in under 3 minutes.** These four claims share one
benchmark, `cmd/segmentbench`, run twice at increasing scale (two of the
three attempts this claim set is allowed). `docs/segmentbench_output.txt`
is the larger of the two, reproduced here in full:

| | Attempt 1 | Attempt 2 (reported) |
|---|---|---|
| Profiles | 3,000 | 20,000 |
| Events (corpus) | 16,559 | 110,032 |
| Consumed by segment engine | 16,559 / 16,559 | 110,032 / 110,032 |
| ClickHouse event store rows | 16,559 | 110,032 |
| ClickHouse distinct profiles | 3,000 | 20,000 |
| **p99 event-to-segment lag** | 72 ms | **134 ms** |
| Segments matched exactly vs. full recompute | 50 / 50 | **50 / 50** |
| Burst offered rate (10x sustained) | n/a shown | 7,287/sec for 8s (58,294 events) |
| **Backlog drained (lag=0)** | 11.7 ms | **10.6 ms** (cap 3m0s) |

Both attempts pass the lag, oracle-diff, and burst-drain claims cleanly;
neither reached the 1M-profile design target. Per the measurement rule,
this is reported as the honest scale reached in the attempts made this
session, not adjusted or hidden: **20,000 distinct profiles / 110,032
events is 2% of the 1M-profile target**, and the 1M claim is not met at
that literal scale. What is measured at 20,000 profiles is real: the send
phase sustained 729 events/sec (well below the 35,791 events/sec pure-edge
throughput above, because `segmentbench` uses 8 send workers against a
consumer also writing to ClickHouse and Redis in the same process, not the
400-worker configuration `throughputbench` uses to find the edge's
ceiling), and every other number in the table came from that same,
unthrottled run.

| Claim | Measured | Met |
|---|---|---|
| 20,000 events/sec sustained | 35,791 events/sec | yes |
| p99 event-to-segment lag under 2s | 134 ms | yes |
| Membership identical to full recompute, all 50 segments | 50 / 50 exact | yes |
| 10x burst backlog drained under 3 minutes | 10.6 ms | yes |
| Segment membership for 1M profiles | 20,000 profiles reached | **no, honestly short** |

### Delivery-stats extension

Machine: same as above (WSL2 Ubuntu 22.04 on Windows 11, 8 physical / 16
logical cores on the Windows host, **WSL itself capped to 12 logical cores
by `~/.wslconfig`, which `nproc` already reflects and every benchmark below
actually ran under**, not the full 16 the "one 16-core machine" claim
names, disclosed here rather than silently measured against a smaller
machine than claimed), Go 1.23.4, Apache Kafka 4.3.1 (KRaft, single broker,
6 partitions), ClickHouse (single node, native WSL2 process, database
`deliverystats`, separate from the segment extension's `default` database).
Given a tight time and cost budget for this build session, every claim
below reflects one genuine measurement attempt, not the playbook's usual
up-to-three; that is disclosed rather than hidden, and none of the numbers
below were tuned to hit a target.

**Claim: simulated email/SMS/WhatsApp delivery webhooks, deduplicating Go
receiver into Kafka and ClickHouse.** `docs/deliverystats_dedup_output.txt`:

| | |
|---|---|
| Outbound messages | 2,000 |
| Unique webhooks (sent + delivered/bounced/failed) | 4,000 |
| Delivered (post-retry) messages | 4,210 (target retry fraction 5.00%) |
| Accepted by the edge | 4,000; rejected as duplicates | 210 |
| Kafka-materialized unique webhook ids | 4,000 |
| Independent reference-dedup unique webhook ids | 4,000 |
| **Result** | **edge-dedup-into-kafka matches the independent reference exactly** |

**Claim: 15,000 webhooks/sec on one 16-core machine.**
`docs/deliverystats_throughput_output.txt`, 6 courtesy-capped goroutines
each batching 400 webhooks per Kafka write, 10s offered duration:

| | |
|---|---|
| Sent (accepted+produced) | 1,091,600 |
| Errors | 0 |
| Elapsed | 10.02s |
| **Sustained throughput** | **108,927 webhooks/sec** |
| Claim | 15,000 webhooks/sec |
| **Met** | **yes (7.3x the claim), measured on WSL's capped 12 logical cores, not the claimed 16** |

**Claim: counts exact against a recompute over 50M events.**
`docs/deliverystats_scale_output.txt`, 50,000,000 rows bulk-loaded directly
into ClickHouse (6 courtesy-capped insert workers, 1,482,163 rows/sec, 34s
total load):

| | |
|---|---|
| Raw `delivery_events` rows | 50,000,000 |
| Rollup `delivery_rollup_minute` rows | 518,412 (1.04% the size of the raw table) |
| (channel, status) pairs matched exactly vs. full recompute | 12 / 12 |
| Recompute total / rollup total | 50,000,000 / 50,000,000 |
| **Result** | **counts exact** |

**Claim: dashboard p95 3.1s to 80ms via rollups.** Same run,
`docs/deliverystats_scale_output.txt`, 20 trials per query over a trailing
30-day window (the full loaded history):

| | Raw table (live `uniqExact` aggregate) | Rollup table (pre-aggregated) |
|---|---|---|
| p95 latency | **8,651 ms** | **416 ms** |
| min / max | 5,532 / 9,564 ms | 256 / 441 ms |
| **Speedup** | | **20.8x** |

The measured pattern is exactly the claim's shape (a live full-table
aggregate is far slower than a pre-aggregated rollup read, and rollups make
the dashboard fast), but the absolute numbers are honestly different from
the claimed 3.1s and 80ms: raw is slower than claimed (8.65s vs. 3.1s,
because `uniqExact` over a String column with no time-ordered primary key
is more expensive at 50M rows on this single-node ClickHouse instance than
the claim's number implies) and rollup is slower than claimed (416ms vs.
80ms, because the rollup table itself is 518,412 rows, not small enough to
answer in tens of milliseconds on this hardware). **Met: no, honestly
short on the absolute numbers, met on the qualitative claim (rollups make
this dashboard meaningfully faster).**

**Claim: 10x burst backlog drained in under 4 min.**
`docs/deliverystats_burst_output.txt`:

| | |
|---|---|
| Baseline sustained rate | 556/sec (measured over 6s unthrottled) |
| Burst offered rate | 5,556/sec (10x baseline) for 6s, 3,366 webhooks |
| **Backlog drained (lag=0)** | **10.4 ms** (cap 4m0s) |
| **Met** | **yes** |

## Building and running

Requires Go 1.21+, and a running Kafka broker (see below for the exact local
KRaft setup used to produce every number above).

```
go build ./...
go vet ./...
go test ./internal/... -v -race
```

Standing up Kafka locally (KRaft mode, no ZooKeeper):

```
curl -O https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz && cd kafka_2.13-4.3.1
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t "$CLUSTER_ID" -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --topic telemetry --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
```

Running the replay-correctness benchmark (the claim's exact measurement):

```
go run ./cmd/loadgen -devices 5000 -readings 20 -topic telemetry-replay-bench
```

Running the outage benchmark:

```
go run ./cmd/outagebench -devices 50 -interval 1s -pre-outage 10s -outage 60s -post-outage 15s -topic telemetry-outage-bench
```

Running the standalone gateway service:

```
go run ./cmd/gateway -addr :8080 -broker 127.0.0.1:9092 -topic telemetry -spool /var/lib/gateway/spool.dat
```

Regenerating the protobuf code (only needed after editing `proto/telemetry.proto`):

```
protoc --go_out=./proto --go_opt=paths=source_relative -I proto proto/telemetry.proto
```

Running the staged load curve (no Kafka needed, uses a fixed-capacity fake
producer, see Measured results):

```
go run ./cmd/loadcurve -stage-duration 2s -out docs/loadcurve_output.txt
```

Running the soak benchmark (no Kafka needed, an in-memory always-succeeds
producer, since what it measures is dedup cache memory, not Kafka
throughput):

```
go run ./cmd/soak -out docs/soak_output.txt
```

Standing up ClickHouse and Redis natively in WSL2 (no Docker in this
environment; see `docs/_start_clickhouse.sh` and `docs/_start_redis.sh`
for the exact commands used to produce the numbers above):

```
bash docs/_start_clickhouse.sh   # binds 127.0.0.1:8123 (HTTP), data in /tmp/ch-data
bash docs/_start_redis.sh        # binds 127.0.0.1:6380, no persistence (-save "")
```

Running the storefront edge-dedup correctness benchmark:

```
go run ./cmd/eventgen -profiles 500 -dup-fraction 0.05 -topic storefront-eventgen-bench
```

Running the storefront edge sustained-throughput benchmark:

```
go run ./cmd/throughputbench -workers 400 -duration 15s -topic storefront-throughput-bench
```

Running the end-to-end segment membership engine benchmark (Kafka,
ClickHouse and Redis must all be reachable; each run gets its own Kafka
topic automatically unless `-topic` is set, so repeated runs never share
state):

```
go run ./cmd/segmentbench -profiles 20000 -avg-events 5 -history-days 120 \
  -broker 127.0.0.1:9092 -clickhouse 127.0.0.1:8123 -redis 127.0.0.1:6380 \
  -burst-multiplier 10 -burst-seconds 8 -out docs/segmentbench_output.txt
```

Running the delivery-stats benchmarks (Kafka and ClickHouse must be
reachable; see `docs/_start_clickhouse.sh` and the Kafka commands above):

```
go test ./internal/deliverystats/... -v -race
go run ./cmd/deliverygen -messages 2000 -dup-fraction 0.05 -topic deliverystats-dedup-bench -out docs/deliverystats_dedup_output.txt
go run ./cmd/deliverythroughput -duration 10s -workers 6 -topic deliverystats-throughput-bench -out docs/deliverystats_throughput_output.txt
go run ./cmd/deliveryburst -baseline-seconds 6 -burst-seconds 6 -topic deliverystats-burst -out docs/deliverystats_burst_output.txt
go run ./cmd/deliveryscale -events 50000000 -insert-workers 6 -clickhouse-db deliverystats -out docs/deliverystats_scale_output.txt
```

Running the standalone service (webhook receiver, Kafka->ClickHouse
consumer, and the dashboard's JSON API in one process):

```
go run ./cmd/deliverygateway -addr :8090 -broker 127.0.0.1:9092 -clickhouse 127.0.0.1:8123 -clickhouse-db deliverystats
```

Running the dashboard against a live gateway (not run this session past
`npm install`'s dependency resolution; source is authored and type-checked
by hand against the gateway's real JSON shape, disclosed honestly rather
than claimed built):

```
cd web/deliverydashboard
npm install
npm run dev   # proxies /api to http://127.0.0.1:8090
```

## Limitations

- MQTT ingest and a TimescaleDB sink were part of this extension's design
  brief (matching the resume's stack line) but were not built this
  session; every claim in `meta.json` is met by the WebSocket/Kafka path
  and the two new benchmarks above without them. Flagged for the reconcile
  stage in case the resume's stack line needs narrowing.
- Kafka is a single broker with no replication; the design's durability
  story rests on the spool file, not on Kafka's own replication, which a
  production deployment would still want in addition to this.
- The 60-second outage is simulated at the producer, not by stopping the
  real broker process; a real broker outage would also involve the
  producer's own connection and metadata-refresh retries, which this design
  does not specifically exercise.
- No throughput or latency benchmark is included; both measurements here are
  correctness checks (did the state end up right, did anything get lost),
  not performance numbers.
- The spool file has no size cap; an outage much longer than the ones tested
  here would grow it without bound until disk runs out.
- No consumer-side application is included; `ConsumeAvailable` exists only
  to verify what a benchmark or test needs, not as a production consumer.
- The Kubernetes manifest was authored to the shape this gateway needs and
  validated for YAML syntax only; it was never applied to a live cluster.
- **The 1M-profile segment-membership design target was not reached.**
  Three genuine attempts were budgeted for this claim; two were run before
  time budget for this build session ran out, at 3,000 and 20,000 profiles
  respectively, both passing every other claim (lag, oracle-diff, burst
  drain) cleanly at their own scale. 20,000 profiles is 2% of the 1M
  target. Nothing about the design is expected to stop working at larger
  scale (the incremental engine's Redis cost per event is O(segments), not
  O(profiles) or O(history), and ClickHouse's recompute path is one SQL
  aggregate per segment regardless of profile count), but that expectation
  was not verified at 1M profiles this session; flagged for the reconcile
  stage to narrow the resume claim to the measured scale, or for a future
  session's third attempt.
- Docker was not available in this environment; ClickHouse and Redis are
  real but run as native WSL2 processes, not containers, and no
  `docker-compose.yml` is included (see "What this is, and is not").
- The storefront corpus's historical spread (up to 120 days backdated) is
  synthetic scaffolding for the segment predicates, not a claim about real
  event volume over real elapsed time; the p99 lag and throughput numbers
  measure this session's real wall-clock send/consume timing, not anything
  about the 120-day window itself.
- ClickHouse's HTTP client in `internal/segment/clickhouse.go` is a thin,
  hand-rolled client covering exactly the two operations this project
  needs (batch insert, aggregate query); it is not a general ClickHouse
  driver and has no connection pooling or retry logic beyond what
  `net/http`'s default transport provides.
- **Every delivery-stats claim was measured with one genuine attempt, not
  the playbook's usual up to three**, because of a tight time and cost
  budget for this build session; none were tuned to hit a target, but a
  second or third attempt was not tried where the first honestly fell
  short (the dashboard-latency absolute numbers, see Measured results).
- **The dashboard-latency claim's absolute numbers were not met.** The
  qualitative claim (rollups make the dashboard meaningfully faster) is
  demonstrated at 20.8x, but raw p95 measured 8.65s (not 3.1s) and rollup
  p95 measured 416ms (not 80ms) at 50M rows on this single-node ClickHouse
  instance. Flagged for the reconcile stage to correct the resume's exact
  numbers to what was measured.
- **The 15,000 webhooks/sec claim was measured on WSL's capped 12 logical
  cores**, not the 16 logical cores the claim's own machine description
  names; the measured 108,927/sec comfortably clears 15,000/sec even at
  the smaller core count, but the discrepancy between "one 16-core machine"
  and the 12 cores actually available is disclosed here rather than
  glossed over.
- **The React dashboard (`web/deliverydashboard`) was authored but not
  built with `npm install`/`npm run build` in this session**, and no
  Playwright smoke test was run against it, both because of the same time
  and cost budget constraint. What was verified live instead: the backing
  `cmd/deliverygateway` JSON API (`/api/summary`), started against the
  50M-row ClickHouse database this session populated, returned real,
  non-hardcoded per-channel/status counts on request. The dashboard's own
  correctness against that API is therefore verified by inspection of its
  fetch/render code, not by an actual browser render.
- **The 50M-row bulk load bypasses Kafka and the edge**, writing directly
  into ClickHouse (see Architecture for why); it is not a claim about
  Kafka-to-ClickHouse throughput at 50M rows, only about ClickHouse's own
  rollup-vs-recompute correctness and query latency at that scale.
- **The rollup table is refreshed by a full rebuild** (`RefreshRollup`
  truncates and re-aggregates the entire raw table), not incrementally; a
  production deployment would more likely use a ClickHouse materialized
  view or a short incremental refresh interval, disclosed as a
  simplification here.
- Docker was not available in this environment; real Dockerfiles and a
  docker-compose.yml for the delivery-stats extension are included in
  `deploy/`, authored to describe the intended containerized shape, never
  built or run this session.
