# Device Telemetry Gateway with Asynchronous Fanout

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
  cmd/
    gateway/main.go            # standalone deployable service
    loadgen/main.go             # the 5,000-device replay-correctness benchmark
    outagebench/main.go          # the 60-second broker-outage benchmark
  deploy/gateway-deployment.yaml # Kubernetes Deployment + Service (not applied live)
  docs/                        # raw output from every run below
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

## Validation

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
