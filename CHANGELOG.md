# Changelog

All notable changes to `babelqueue` (Python) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.23.0] - 2026-10-25

Implements ADR-0036 (post-R1 parity) decisions 2, 4, 5 and 6. A MINOR: every change is a
consumer-side settle decision, so the envelope stays at `schema_version: 1`. The vendored
conformance suite moves to v1.7.0 (new sections `identity_verdict` and `pulsar_exhaustion`, both
passing; `conformance-known-gaps.json` stays empty).

### Changed (behaviour)
- **A body that does not decode, or has no identity, is poison on every transport (ADR-0036
  decisions 4 and 6).** A JSON object that has neither a valid string `job` nor a valid string
  `urn` (ADR-0032 section 6) used to be an *unknown URN*, and on SQS, AMQP, Pulsar, Azure Service
  Bus, Artemis and the memory transport a body that did not decode was decoded leniently into an
  empty envelope and retried as an unknown URN, finally dead-lettered as `reason: "unknown_urn"`
  without the original body. Both are now **poison** and take the poison route of Go's
  `routePoison`: they are never handed to a handler and never take the unknown-URN strategies
  (those remain for a valid envelope whose URN has no handler). With the DLQ on (`dead_letter=True`
  or `on_unknown_urn=DEAD_LETTER`) the message is written to the DLQ at once, with no retry, as the
  ADR-0035 record (`dead_letter.reason: "poison"`, the original body preserved: a strict JSON
  object spliced byte for byte, anything else as `raw`, base64 plus `raw_encoding` when it is not
  valid UTF-8) and acknowledged after the write; `on_unknown_urn=DELETE` drops it (logged); with
  the DLQ off `RELEASE` is replaced by the bounded `FAIL` path (re-published with `attempts + 1`
  up to `max_attempts`, then dropped and logged). An operator who relied on a poison body being
  retried through the unknown-URN strategy, or on a `reason: "unknown_urn"` record for it, sees the
  new behaviour. The classification is `EnvelopeCodec.poison_cause(raw)` (new, pure): the typed
  decode of the Go reference plus the identity rule; `decode` / `decode_lenient` are unchanged.
- **Pulsar poison records carry `reason: "poison"`** (they were `unknown_urn`). The Pulsar
  transport also reports the reconciled `bq-attempts` / redelivery count to the runtime
  (`ReceivedMessage.native_attempts`, new), as does SQS (`ApproximateReceiveCount - 1`), so a poison
  body, which cannot carry `attempts`, is still bounded by `max_attempts`.
- **Redis: an identity-less body is dead-lettered unconditionally (ADR-0036 decision 4,
  broker-bindings section 1.1 "Identity-less body").** In both layouts (legacy `:processing`
  and Laravel-compatible zset) a body that decodes but names no identity (including a bare `null`,
  which used to follow the bounded `FAIL` path) is handed over as transport poison: it is written
  to `<queue>.dlq` as the ADR-0035 record with `reason: "poison"` in every configuration
  (`on_unknown_urn=DELETE` and dead-lettering off change nothing), never handed to a handler and
  never retried. Nothing else in the Redis transport changes.
- **Kafka without retry tiers: a poison record no longer blocks the partition (ADR-0036 decision
  5, broker-bindings section 6.5 degrade-to-delete).** With no `retry_tiers_ms`, dead-lettering off
  and `on_unknown_urn` not `DELETE`, the bounded retry of a poison record (including an
  identity-less body) needs a tier and raised, leaving the record uncommitted with `bq-attempts`
  never advancing. It is now committed, dropped and logged, and the partition advances. A failing
  handler and `RELEASE` of a valid envelope keep the section 6.5 rule (they still raise
  `BabelQueueError` without tiers). The start-up warning for a tierless transport now says so; enable
  `dead_letter` to keep such records in `<topic>.dlq`.
- A message dropped after its retries are exhausted with no DLQ configured is now logged
  (`babelqueue.app`, WARNING) instead of being dropped silently.

- **Pulsar: a failed DLQ write or retry publish is negatively acknowledged (Go `Rewind`).**
  `PulsarTransport` is now a `Rewinder` (`negative_acknowledge`), so the unsettled original is
  redelivered by the broker instead of staying un-acknowledged for the life of the consumer. A
  graceful-shutdown release of an in-flight Pulsar message is now a nack as well rather than a
  re-publish (on a `Shared` subscription the native redelivery count advances; on `Exclusive` /
  `Failover` it does not, which is harmless: `bq-attempts` carries the attempt limit).

### Fixed
- **Bytes that are not valid UTF-8 no longer crash the AMQP, Pulsar and Artemis consumers.** They
  were decoded strictly in `pop`; they are now kept losslessly (`surrogateescape`). Invalid UTF-8
  is poison wherever it sits, **also inside a JSON string** (`EnvelopeCodec.poison_cause`,
  error-handling section 7): it is never handed to a handler and is dead-lettered as the ADR-0035
  base64 record, like on Redis and Kafka.

### Added
- `EnvelopeCodec.poison_cause(raw, *, require_identity=True)`; `Transport.classifies_poison`
  (Kafka and Redis classify poison themselves and opt out of the runtime's classification);
  `ReceivedMessage.native_attempts`.
- Conformance runners for `identity_verdict` (the core verdict on all 97 bodies, plus the fate on
  the memory transport) and `pulsar_exhaustion` (all 29 rows, driven through the real
  `PulsarTransport` and runtime over a duck-typed client), and per-transport poison tests.
- `ci.yml` / `release.yml` pin conformance `1.7.0` (the `fetch.sh` SHA-256 is unchanged).

## [1.22.0] - 2026-10-24

Implements ADR-0033 (Redis) and ADR-0034 (Kafka tiers, backoff, poison). The envelope stays at
`schema_version: 1`; the vendored conformance suite moves to v1.6.0 and
`conformance-known-gaps.json` is now empty.

### Changed
- The Redis poison dead-letter element is now the ADR-0035 record (raw splice, or the `raw`/`raw_encoding` wrapper) carrying a `dead_letter` block, instead of the bare element (broker-bindings section 1). A legacy `__bq_frame` header is unwrapped, not re-applied. Dropped the dead `_reject_json_constant` helper in the Kafka transport.
- **Redis: a stored value that is not valid UTF-8 no longer crashes the reserve.** The client built from a URL now uses `encoding_errors="surrogateescape"`, replies are decoded losslessly, and the settle / release / dead-letter calls send the exact stored bytes back, so such a poison body becomes the ADR-0035 base64 wrapper (no U+FFFD) and its reservation is actually removed. A client you pass in with `decode_responses=True` should set `encoding_errors="surrogateescape"` as well (redis-py's default strict decoding raises on such a reply).
- **Poison DLQ record shape follows ADR-0035** (Kafka): a strict JSON object body is spliced byte for byte with a `dead_letter` member, anything else is wrapped as `{"raw": ..., "raw_encoding"?: "base64", "dead_letter": ...}`; a body that is not valid UTF-8 is stored losslessly as base64 instead of with U+FFFD, and the conformance `poison_dlq` section runs.

### Fixed
- **Invalid UTF-8 is poison in both Redis layouts (broker-bindings section 1.1).** A stored value (or zset `:reserved` member) that is not valid UTF-8 is dead-lettered on first delivery as the ADR-0035 `raw`/`raw_encoding: "base64"` wrapper, even when the codec or `cjson` could decode it: it is never handed to a handler and never retried, released or re-published, and the exact stored bytes are used for the settle script, so no byte is rewritten.
- **Redis, Laravel-compatible layout: a poison reservation is dead-lettered in every
  configuration.** The DLQ write no longer depends on `dead_letter_enabled` (default `False`) or
  `on_unknown_urn`: an undecodable body or one without a numeric `attempts` is written raw to
  `<queue>.dlq` and acked instead of being logged and dropped (broker-bindings section 1.1,
  ADR-0033; same as the Go reference). Other version-gated routes keep their previous behaviour.
- **Redis, Laravel-compatible layout: a job with a non-numeric `attempts` or an undecodable body no
  longer aborts the reserve script.** It is reserved raw (`pcall` guard), dead-lettered byte for
  byte and acked, leaving nothing in `:reserved` (ADR-0033 (c)).
- **Redis, legacy layout: a `kill -9` no longer strands a message in `<queue>:processing`.** Every
  reservation writes a `<token>:<raw>` member into `<queue>:processing:deadlines`; a sweep before
  each reserve moves lapsed reservations back to the end of `<queue>`. Ack, release and dead-letter
  are gated on their own reservation, so a late ack never touches a newer one. The legacy mode
  remains the default and the `<queue>:processing` key name is unchanged.
- **Kafka without retry tiers: a failing handler no longer stops the worker** (Go
  `leaveRepublishUnsettled` parity). The transport still raises `BabelQueueError` (section 6.5), but
  the runtime now catches a failed retry release, logs it (`babelqueue.app`, ERROR, "NOT
  acknowledged"), leaves the record unsettled (no commit; a rewind-capable transport seeks back and
  the loop pauses `_rewind_pause` first) and keeps consuming. Previously the error escaped
  `consume()`. Applies to any transport whose retry release fails.
- **Kafka poison follows Go `routePoison` when no DLQ is configured.** With the DLQ on
  (`dead_letter=True` or `on_unknown_urn=DEAD_LETTER`) the wrapped record is written to
  `<topic>.dlq` (`dead_letter.reason: "poison"`) and then committed. With the DLQ off the poison
  record is **not** written anywhere: `on_unknown_urn=DELETE` drops it, every other strategy takes
  the bounded FAIL path (retried through the retry tiers up to `max_attempts` with a bare
  `{"attempts": N}` body, then dropped; without tiers the record is redelivered in place).
  Operational note: when the DLQ is on, `<topic>.dlq` must exist; a failed write leaves the record
  uncommitted and rewound (it is never dropped).
- **Unknown-URN `RELEASE` and the retry release leave the delivery unsettled on failure** (Go
  `leaveRepublishUnsettled`): a failed re-publish of an unknown-URN message is no longer counted as
  a handler failure and can no longer lead to the record being acked and lost.
- **Redis, legacy `:processing` layout: an undecodable body is dead-lettered on its first
  delivery.** A body Go's typed `Decode` rejects (neither a JSON object nor `null`, or wrong member types such as
  a string `attempts`/`meta`; same criterion as Kafka poison) is written verbatim to `<queue>.dlq`
  through the gated settle script and removed from `:processing`, in every configuration, instead of
  being retried until `max_attempts`. A valid JSON object without an identity keeps the existing
  bounded path.
- **Kafka: a record without a valid `job` / `urn` is poison** (ADR-0034 section 10), not an
  unknown URN: with the DLQ on it is dead-lettered wrapped with `dead_letter.reason: "poison"` and
  never retried; with the DLQ off it follows Go `routePoison` (see above).

- **Redis, Laravel-compatible layout: the reserve script no longer aborts on a body that decodes
  but cannot be re-encoded** (`cjson.encode` of `inf`/`NaN`, e.g. `1e400`, raised after the `LPOP`
  and lost the job). The encode is now inside `pcall`; the job is reserved raw and takes the poison
  path. Note: fakeredis' `cjson` serialises `inf` as `Infinity` and masks this; it is proven by a
  real-Redis test (CI `redis:7`) and by a Lua test against a failing `cjson` stub.
- **`on_unknown_urn="delete"` no longer discards a Redis poison reservation.** It is dead-lettered
  byte for byte like the Kafka poison record already was; a genuine unknown URN is still deleted.
- **Redis, Laravel-compatible layout: `dead_letter` is atomic and gated.** One script `ZREM`s the
  reservation and writes the dead-letter record and its notify entry; an expired reservation writes
  nothing (and logs a warning) instead of publishing a duplicate.
- **A message dropped because it is undeliverable and no dead-letter queue is configured is
  logged** (`babelqueue.app`, WARNING) instead of disappearing silently.
- **Kafka `RetryReinjector` no longer loses records:** it kept one held record per partition, so a
  second record on the same partition overwrote the first. It now keeps all of them in order.
- **A worker on a tier-less Kafka transport with `max_attempts` > 1 warns at start-up** that a
  failing handler cannot be retried: the record is left uncommitted and redelivered.
- Internal: the legacy `:processing` settle script takes `[token, raw]` and builds the deadline
  member itself.

### Added
- `RedisTransport(retry_after=90, migration_batch_size=-1)` (also `?retry_after=` in the URL;
  `None` / `null` disables recovery). The Laravel-compatible `retry_after` default is now 90 s
  (was 60 s).
- SQS: `SqsTransport.publish(..., delay_seconds=)` (`DelaySeconds` 0..900; out of range or FIFO
  raises `BabelQueueError`).
- Kafka: `KafkaTransport(retry_tiers_ms=...)` with `DEFAULT_RETRY_TIERS_MS`, `retry_topic()` and
  the `babelqueue.kafka_retry.RetryReinjector` consumer that moves parked records from
  `<topic>.retry.<n>` back to the work topic (broker-bindings section 6.4). Opt-in: without tiers a
  retry / `publish_delayed` raises `BabelQueueError` (the transport raises; the worker logs it and
  leaves the record uncommitted, see Fixed).
- Worker-side retry backoff: `BabelQueue(retry_backoff=, retry_backoff_multiplier=,
  retry_backoff_max=, retry_jitter=)` (ADR-0034 section 9); the defaults keep an immediate release.
- Conformance runner covers the v1.6.0 sections (`redis_layout`, `redis_delay`, `retry_backoff`,
  `kafka_poison`, ...). CI runs the Redis cases against a real `redis:7` with `BQ_REQUIRE_REDIS=1`.

### Changed
- **Kafka retries go to the retry tiers.** With `retry_tiers_ms` set a failed message is parked on
  `<queue>.retry.<n>` and needs a running `RetryReinjector`; the retry topics must exist. Without
  tiers a retry now raises from the transport instead of republishing onto the work topic; the
  runtime logs it and redelivers the record rather than stopping the worker.

### Documentation
- README: Redis layouts, the limit that the Laravel-compatible layout carries no out-of-band headers (ADR-0033 §5),
  Kafka retry tiers, `LogAppendTime` on the tier topics, retry backoff, SQS `delay_seconds`.

## [1.21.1] - 2026-10-20

### Fixed
- **Azure Service Bus: `publish` no longer fails with `Value supplied for int invalid`.** The pure-Python AMQP
  stack in `azure-servicebus` encodes a bare Python int as AMQP int32, so the millisecond `bq-created-at`
  application property (broker-bindings.md §4.3) overflowed at send time. Out-of-int32 integer properties
  are now sent as AMQP `long`, matching the Go, Java, .NET and Node transports. The native projection and
  the envelope body are unchanged.

## [1.21.0] - 2026-10-19

Implements ADR-0032 (conformance packaging, results format, ASCII trim and the non-string `job` rule).
The envelope stays at `schema_version: 1`.

### Changed
- **`EnvelopeCodec.decode` raises `BabelQueueError` for a JSON object with neither a valid string `job` nor
  a valid string `urn`** (ADR-0032 section 6). Malformed or non-object input still yields `{}`. The runtime
  dispatch, the transports' attempts reconciliation and `redrive` use the new `decode_lenient`, so such a
  body still takes the poison / pass-through path.
- A non-string `job` (`None`, number, boolean, object, array, falsy or not) is ignored with a warning and a
  valid string `urn` takes over.
- Trim and blank checks in `make` (`urn`, `trace_id`) and `accepts` (`trace_id`) use the closed ASCII set
  (space, `\t`, `\n`, `\v`, `\f`, `\r`); `str.strip()` Unicode whitespace is no longer trimmed
  (ADR-0032 section 5). The set is exported as `codec.ASCII_WHITESPACE` and the transports import it.

### Added
- `EnvelopeCodec.decode_lenient`.
- Conformance runner covers the v1.5.0 sections and writes `results.json` (ADR-0032 section 2).
  `conformance-known-gaps.json` and `scripts/check_conformance_results.py` gate it in CI and release: the
  SQS `delay` and Kafka `backoff` tier cases run and record honest `fail`s until they are implemented.

## [1.20.0] - 2026-10-17

Conformance suite 1.4.0, [ADR-0031](https://babelqueue.com). The envelope is unchanged
(`schema_version: 1`); this is a MINOR with one deliberate behaviour change (encode rejects).

### Added
- `EnvelopeCodec.make(urn, data, *, queue, trace_id, meta=None)`: a producer `meta` write API. The entries
  land in `meta` next to the core keys (recommended shape `meta.ext.<vendor>`). A collision with a core
  meta key or a forbidden key raises the new `EnvelopeKeyError` (subclass of `BabelQueueError`, with a
  `pointer` attribute). `_envelope_shape.CORE_META_KEYS` is the single copy of the core key list.
- `decode` folds the `urn` alias into `job` (ADR-0031 (c)): a `job` that is missing, empty or only ASCII
  whitespace (`" \t\n\v\f\r"`; a U+2003 `job` is NOT blank) falls back to `urn` silently; when both carry
  a value, `job` wins and `urn` is dropped with a warning naming `/urn`. `accepts()` / `urn()` follow the
  same rule. The decoded envelope never keeps a `urn` key.
- Tests: the runner now implements the 1.4.0 fields (`expect_preserved` on `forbidden_keys`;
  `expect_absent_after_reencode`, `apply_dead_letter`, `expect_dead_letter` on `roundtrip`, the latter via
  `dead_letter.annotate`) plus the `extension_write` and `encode_reject` sections, with a runner
  self-test proving violations are reported.

### Changed
- **Behaviour change: `EnvelopeCodec.encode` raises `EnvelopeKeyError`** for an envelope carrying a
  forbidden key (message-envelope.md section 10) or a top-level `urn`, instead of dropping it with a
  warning (policy K-15 (a)). Nothing is written. `decode` still warns and drops, so re-encoding a decoded
  envelope is safe. Code that built an envelope from a raw legacy body and relied on encode to clean it
  must decode it first (`EnvelopeCodec.encode(EnvelopeCodec.decode(raw))`).
- `test_all_five_contract_keys_are_covered` is a subset check (the suite adds `/urn` cases).
- Docs only: the note in the previous release that the Go reference's case-insensitive envelope-key read "is tightened separately" is now closed — Go core 1.14.0 reads envelope keys exactly (case-sensitive, last duplicate wins), so every SDK now decides poison on the same exact-case keys.

### Fixed
- Raw-body re-encode paths now decode before they encode, so the new `encode` rejection cannot strand a
  legacy body: the runtime's raw-body RELEASE re-publish (`app.py`), and the SQS and Pulsar attempts
  reconciliation (Kafka, Azure Service Bus and Artemis already decoded first). A body with a legacy
  `timestamp` or only a `urn` alias is re-emitted normalised (forbidden key dropped and warned, `urn`
  folded into `job`); unknown top-level and `meta` keys, including `meta.ext`, are preserved.

## [1.19.0] - 2026-10-12

### Changed
- Tests only: the vendored conformance suite is now 1.3.0 (exact-case envelope keys, last-wins duplicates: new `invalid-wrong-case-job`, `wrong-case-keys-roundtrip` and `duplicate-keys-last-wins` fixtures). No production change; the Python core already behaved this way.
- **Kafka: a poison record is dead-lettered wrapped (`dead_letter.reason: "poison"`), like the Go
  reference (§6.5, ADR-0009).** A value that is not valid UTF-8, not valid JSON, not a JSON object, or has
  a member of the wrong type (the exact set is listed in the next entry) used to reach `<topic>.dlq` as its raw
  bytes. It is now written as a body that keeps its keys when it is exactly one JSON object, otherwise
  `{"raw": "<body>"}` (bytes that are not UTF-8 are stored with U+FFFD substitutions), plus a
  `dead_letter` block (`reason: "poison"`, `original_queue`, `attempts` from the `bq-attempts` header, `lang`);
  the record key is preserved. A poison record is still never retried and is dropped when no DLQ is
  configured (the `on_unknown_urn` strategy, `DELETE` included, no longer matters: see below). The version-gate rejection (`bq-schema-version`, §6.7) still
  dead-letters its record raw. The wrap is applied when the runtime writes the DLQ record (not when the
  record is read), so a poison record that `redrive` restores into the DLQ it popped it from stays byte
  for byte, whatever that DLQ is named. `original_queue` is the `bq-original-topic` header when present
  (else the consumed topic), like the Java, .NET and Node consumers; a body whose numbers cannot be
  written back as valid JSON (e.g. `1e400`) is carried as `{"raw": ...}` instead of producing an invalid DLQ body.
  A body that decodes but names no URN is not poison (unknown-URN path, as before). Consumers of the DLQ
  that expected the raw bytes of a malformed record must now read the wrapped body.
- **Kafka: the poison trigger set now equals what the Go reference's typed `Decode` rejects.** Behaviour
  change; the old Python-only rules (a lenient `attempts` read, `Accepts`-style conformance checks) are gone.
  A record is poison when it is not valid UTF-8, not valid JSON (an empty or whitespace-only body, a BOM,
  trailing data, leading zeros, `NaN` / `Infinity`, single quotes), or not a JSON object (a bare `null` is
  not poison: it is an empty envelope and takes the unknown-URN path), or when a member has the wrong type:
  `job`, `trace_id`, `meta.id|queue|lang` and `dead_letter.reason|error|exception|original_queue|lang`
  must be a string or null; `meta.schema_version`, `meta.created_at`, `attempts` and
  `dead_letter.failed_at|attempts` must be an int64 integer literal or null (a string such as `"3"` or
  `"2026"`, a fraction or exponent such as `3.0` / `1e2`, a bool, an array and an object are poison);
  `data`, `meta` and `dead_letter` must be an object or null; every number inside `data` must be a finite
  double. Member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read them; a `JOB` or `Meta` member is an unknown member, not a typed one) and every duplicate key is checked. A record Go decodes
  is no longer poison, even when it is not a conformant envelope: a non-string `urn` alias, an absent
  `data`, `"attempts": null` (read as 0 and written back as 0) and `meta.schema_version: 2` are routed by URN
  like any other record (Go's consume path never calls `Accepts`). The Kafka transport no longer
  delivers records under the shared non-numeric-`attempts` route; they are plain poison now.
  `redrive` restores a poison-routed record into the DLQ unchanged instead of replaying or re-wrapping it,
  even when its `job` still reads as a URN. The only Python-specific limit is JSON nested deeper than
  the interpreter's recursion limit (about 1000 levels, Go allows 10000), which is treated as poison.
- Known, intentional differences from the Go reference for a poison record: `dead_letter.attempts` is the `bq-attempts` header (Go writes 0) and `original_queue` is the `bq-original-topic` header when present (Go writes the consumed topic); a float literal such as `1.0` / `1e2` is re-formatted when the body is re-encoded. The wire envelope and the `dead_letter` shape are unchanged.
- **Kafka: any decode failure is poison; the partition is never left to wedge.** Member names are matched case-sensitively, exactly as the core decoder reads them (no `encoding/json`-style folding, no `str.casefold()`); the Go reference's case-insensitive read is a known difference that contradicts the contract schema and is tightened separately. Whatever `EnvelopeCodec.decode` raises after the shape scan (e.g. `RecursionError` at the interpreter's limit) is now routed as poison instead of escaping `pop` (which left the offset uncommitted and the partition redelivered forever); the poison wrapper falls back to `{"raw": ...}` on `RecursionError` or a body that cannot be written as UTF-8. A lone surrogate escape (`"\ud800"`) that survives into a retry/DLQ write is substituted by U+FFFD, as Go does, instead of raising `UnicodeEncodeError`. Invalid UTF-8 remains poison (Go substitutes U+FFFD and accepts it). The wire envelope and the `dead_letter` shape are unchanged.
- **Kafka: a body the core JSON parser refuses is poison, not an empty envelope.** The core decoder turns a `ValueError` from `json.loads` into an empty envelope; where the shape scan accepts a body that plain `json.loads` rejects (Python 3.11+: an unknown member holding an integer of more than 4300 digits), the record would have reached a handler with its content dropped. It is now dead-lettered as poison. No change to the wire envelope or the `dead_letter` shape.
- **Kafka: a poison record is dead-lettered whatever `on_unknown_urn` is.** The runtime checked the `DELETE` strategy before it recognised a poison record, so with `on_unknown_urn="delete"` a poison record was acknowledged and silently dropped even with a DLQ configured, where the Go reference writes it to `<topic>.dlq` with `dead_letter.reason: "poison"` and commits. A poison delivery (`PoisonRoute`) is now settled before the strategy is consulted: written wrapped to the DLQ and committed under `fail`, `delete`, `dead_letter` and `release`, and dropped only when no DLQ is configured (unchanged). The version-gate path and every other transport keep their behaviour. No change to the wire envelope or the `dead_letter` shape.

### Fixed
- Kafka: a wrongly-typed `meta` or `meta.created_at` in a wrapped poison body no longer fails the write
  that settles it (header projection and record timestamp tolerate it).

## [1.18.0] - 2026-10-11

### Added
- `BatchPopper` (optional transport capability, exported from `babelqueue`) with
  `pop_batch(queue, max=0, timeout=1.0)`: take several deliveries at once and hold them un-settled.
  `KafkaTransport` implements it; `redrive` uses it when a transport offers it.

### Fixed
- **`redrive` no longer loops forever (default `max=0`) or repeats records (`max>1`) on Kafka.**
  `redrive` drains the DLQ before it processes it, but `KafkaTransport.pop` first seeks back to the
  previous un-acked record, so every pop returned the first record again. The drain now takes the
  batch through `pop_batch` (one rewind up front, none between the polls), and `KafkaTransport`
  tracks several in-flight records: acks are committed in offset order, so the cumulative commit
  never passes a record that is still unsettled, and an abandoned batch is rewound to its lowest
  offset by the next `pop`. The restore writes are not re-read in the same call. Other transports
  and the single-record `pop`/`ack`/`rewind` behaviour are unchanged. No wire change.
  A record whose envelope carries an `attempts` the runtime cannot read as an integer (a foreign
  producer: `null`, `""`, a list/object, a bool, `"three"`, `NaN`/`Infinity`; an absent key is just 0) no longer raises out
  of `pop` or the restore publish. `KafkaTransport.pop` hands it over under a reserved poison route
  (like a non-UTF-8 record), so the runtime dead-letters it raw without running a handler (a failing
  handler can no longer wedge the partition) and `redrive` restores it to the DLQ instead of
  replaying it. The redundant `bq-attempts` header is left out of such a write.
- **`redrive` now carries the DLQ record key onto the redriven record.** The restore writes back to
  the DLQ already kept the key; the redrive write to the work topic did not, so a replayed Kafka
  record lost its key (and per-key partition affinity). It now goes through
  `publish_keeping_route(..., key=message.key)`, byte-for-byte (null stays null, empty stays empty),
  per contract §6.5 "Record key on republish". Non-keyed transports behave exactly as before; the
  `bypass=True` header path still carries no key. No wire change.

## [1.17.0] - 2026-10-11

### Added
- `SettlePublisher` (optional transport capability, exported from `babelqueue`) with
  `publish_settle(queue, body, route_urn="")`: the runtime's settle writes (retry republish,
  envelope / poison / gate-rejection DLQ, `redrive` restore) go through it when a transport
  implements it. `KafkaTransport` implements it.
- `KafkaDeliveryError` (a `BabelQueueError`) and the optional `KafkaTransport(delivery_timeout=30.0)`
  constructor argument.
- `ReceivedMessage.key` (`Optional[bytes]`, `None` = null key) and an optional `key=` argument on
  `SettlePublisher.publish_settle()` / `publish_keeping_route()`. `KafkaTransport.pop()` fills the
  key from the consumed record.

### Fixed
- **Kafka: the original record key now survives the retry republish, the envelope DLQ and the
  raw (poison / gate-rejection) DLQ.** The key was dropped on every settle write, so a keyed record
  lost its key (and with it per-key partition affinity) after the first retry. A null key stays
  null, an empty key stays empty, and a key is never synthesised. `redrive` restores to `<queue>.dlq`
  also keep it. Partition equality is not guaranteed. The envelope and the `dead_letter` block are
  unchanged.
- **Kafka: retry / dead-letter writes are now broker-confirmed before the offset is committed.**
  `KafkaTransport.publish_settle()` waits for this write's own delivery report
  (`flush(delivery_timeout)`). A delivery error or timeout raises `KafkaDeliveryError`, nothing is
  committed, and the record is re-read through the existing rewind/seek path — so such a write can
  no longer be lost to a commit that raced ahead of it. `publish()` (and so `app.publish()`) is
  unchanged: still fire-and-forget and strict (a body that is not valid UTF-8 still raises
  `UnicodeEncodeError`), also from inside a handler.
- **Kafka: a record whose value is not valid UTF-8 is dead-lettered raw instead of raising.** The
  original bytes are written to `<queue>.dlq` unchanged, with no projected headers (as
  Java/Go/Node do for poison records), and the offset is committed only once that write is
  confirmed; with the DLQ off, or `UnknownUrnStrategy.DELETE`, the record is dropped and
  committed. It reuses the version-gate settle path, so a failed write logs as a "version-gated"
  message — the logged route (`...:non-utf8-payload`) tells them apart. A non-UTF-8 `bq-attempts`
  header no longer raises. No wire change.

## [1.16.1] - 2026-10-11

### Changed
- **Schema-version gate rejection now dead-letters the incoming body, byte-identical.** A message
  whose `bq-schema-version` (`bq_schema_version` on Artemis) is unsupported is rejected *without
  decoding it* (broker-bindings §3.7/§4.7/§5.7/§6.7/§7.7) and, when the DLQ is enabled, the DLQ
  record is the raw body as received, with no `dead_letter` block. Previously the core decoded
  the envelope and wrote an annotated `dead_letter{reason:"unknown_urn"}` record. This aligns the
  Python SDK with Java, .NET and Node. A DLQ consumer that expected the annotation for gated
  messages is affected; a failed DLQ write still leaves the message unsettled (rewound on Kafka),
  and DLQ off / `UnknownUrnStrategy.DELETE` still drops it. No wire or API change.

### Fixed
- **Kafka: a record whose settlement raised was silently lost on a consume-loop restart.** When a
  retry / release / dead-letter `publish` failed (or the loop was otherwise interrupted before
  `ack`), `consume()` ended with the record neither acked nor rewound while `poll` had already
  advanced the position; the restarted loop received the *next* record and acking it committed
  past the unsettled one (Kafka offsets are cumulative). `KafkaTransport` now remembers the
  record `pop` handed out and re-seeks it on the next `pop` unless it was acked or rewound.
  No API or wire change.

## [1.16.0] - 2026-10-10

### Added
- New optional `Rewinder` protocol (`rewind(message)`), implemented by `KafkaTransport` and
  exported from `babelqueue` (public API, MINOR). The runtime calls it after a failed
  version-gate dead-letter write and then pauses briefly (1 s) before the next poll.

### Fixed
- **`bq-schema-version` pre-decode gate on the SQS, Azure Service Bus, Kafka and Artemis
  transports** (`pop`). The broker's schema-version property is read **before** the body is
  decoded (broker-bindings §3.7 / §4.7 / §6.7 / §7.7), mirroring the existing Pulsar gate:
  a missing or blank value (empty or only ASCII whitespace) takes the normal path; anything
  else is compared untrimmed to `"1"` and a mismatch is returned with the raw body and the
  reserved route `babelqueue:unsupported-schema-version:<v>` (version percent-escaped) without
  decoding it. SQS/ASB/Kafka read `bq-schema-version`, Artemis `bq_schema_version`
  (ADR-0017); ASB and Artemis also accept an integer `1` (a `bool` is not `1`). The conformance suite now reads each binding's
  `schema_version_gate` table (including Pulsar's). No wire change.
- **A version-gated delivery is dead-lettered directly, never retried.** `BabelQueue` now
  sends a message under the reserved gate route straight to the DLQ (reason `unknown_urn`,
  body untouched) when `dead_letter` is enabled and drops it otherwise, regardless of
  `max_attempts` / `on_unknown_urn`. A retry would re-publish through paths that rebuild the
  version header from the (valid) body (ASB/Kafka/Artemis) or never advance the attempt
  counter (SQS), letting the gate be bypassed or the message loop. Applies to Pulsar too
  (previously it retried up to `max_attempts` first).
  If the dead-letter write itself fails, the message is still never retried: it is left
  unacknowledged (an error is logged) so the broker redelivers it and the gate rejects it again
  (SQS at the visibility timeout, Azure Service Bus at lock expiry, Artemis when the link or
  connection closes). Kafka commits offsets cumulatively, so merely not acknowledging the
  record would lose it once a later record is committed: `KafkaTransport` now `seek`s the
  partition back to the failed record's offset so it is polled again and the dead-letter write
  is retried (no commit moves past it).
- The rejected `bq-schema-version` value is capped at 64 characters plus `...` in the log and
  the route.

## [1.15.0] - 2026-10-10

### Added
- **`ReceivedMessage.route_urn` routing seam** (MINOR). An optional field a transport can set
  when its broker carries the URN out of band. When non-empty `BabelQueue.dispatch()` looks the
  handler up by it instead of the decoded body's URN; empty (the default for every existing
  transport) keeps routing on the body. The body is never rewritten, so the handler, the retry
  re-publish and the dead-letter record carry the producer's original body (including a `urn`
  alias). No wire change.
- **`RoutedPublisher` optional transport capability** (MINOR). `publish_routed(queue, body,
  route_urn)`. When a delivery has a `route_urn` and the transport implements it, the retry, release and dead-letter
  writes, and `redrive`, use it so the broker's routing property keeps the original
  value across attempts (no flip to the body URN on the second delivery). `PulsarTransport`
  implements it (`bq-job` pinned; blank derives from the body). Others keep plain `publish`.
  No wire change.

### Changed
- `BabelQueue.dispatch()` applies `route_urn` only to a body that decodes; an undecodable body
  stays on the poison / unknown-URN path instead of being routed to a handler.

### Fixed
- **`on_unknown_urn=RELEASE` no longer loops a version-gated or poison message forever.**
  `BabelQueue.dispatch()` re-published such a message unchanged on every delivery (`attempts` never
  grew, so it never reached the DLQ). A delivery a transport version-gated (reserved
  `unsupported-schema-version` `route_urn`, e.g. Pulsar `bq-schema-version`), an undecodable body
  and a body naming no URN now take the bounded `FAIL` path under `RELEASE` (retry up to
  `max_attempts`, then dead-letter or drop), matching the Go core (GR-5). A genuinely unknown URN
  still releases verbatim. No public API or wire change. Note: as on the existing `FAIL` path, a
  body that is not valid JSON is not carried verbatim through the retry / dead-letter re-publish
  (the runtime re-encodes the envelope it could build, so the producer's original bytes are lost);
  previously `RELEASE` re-published those bytes unchanged.
- **Pulsar consumer version-gates on `bq-schema-version` (broker-bindings §5.7 step 3).**
  `PulsarTransport.pop()` now reads the `bq-schema-version` message property before decoding the
  body. A missing or blank value changes nothing; "Blank" is the same set in every SDK: the empty string or only ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`); anything else (NBSP, U+001C-U+001F, U+0085, U+FEFF, U+3000, ...) is not blank and is rejected unless it is exactly the supported version. (`str.strip()` is not used: it also strips U+001C-U+001F, U+0085, NBSP, ...) a value other than the supported
  `SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...) is not decoded: the delivery carries the reserved
  `route_urn` `babelqueue:unsupported-schema-version:<value>`, which no handler owns, so the
  runtime quarantines it through the existing unknown-URN retry / dead-letter path (the error
  names the rejected version). The body is passed through untouched. No wire change. The route
  is an in-process marker only: it also carries the delivery's original `bq-job`, and the retry /
  dead-letter re-publish (`publish_routed`) writes that original `bq-job` (or, when the delivery
  had none, the body's job) and the original raw `bq-schema-version` back, never the sentinel, so
  a redrive meets the same gate. The gate also logs a `babelqueue.pulsar` warning naming the
  rejected version and queue, including for a body that is not valid JSON (which the runtime
  quarantines as a poison message whose error names the reserved route).
- **Pulsar consumer now follows broker-bindings §5.2 / §5.7.** `PulsarTransport.pop()` routes on
  the `bq-job` message property (set on `ReceivedMessage.route_urn`; the body's URN is used only
  when the property is absent or blank) and takes `attempts` from the authoritative
  `bq-attempts` property (the body's value is used only when the property is absent or not a
  plain ASCII-decimal integer that fits a 32-bit int), then applies `max(.., redelivery_count)`.
  Previously only the body was read. The body's `job` / `urn` is **not** rewritten, so a
  property/body mismatch no longer leaks into retries or the DLQ; `attempts` is written into the
  body only when the body already holds an integer `attempts` and the value changes (a missing
  or non-integer `attempts` is still rejected by `EnvelopeCodec.accepts`). When the property and
  the body name different URNs the property wins and the new optional
  `on_routing_mismatch(property_urn, body_urn, envelope)` constructor hook is called for an
  accepted envelope, after attempts are reconciled (its exceptions are swallowed; it is separate
  from error handling). A body without a URN is still rejected. No wire change
  (`schema_version: 1`).

## [1.14.0] - 2026-10-03

### Added
- **Graceful shutdown** of `BabelQueue.consume()`: on SIGTERM/SIGINT the loop sets a stop flag,
  takes no new message, lets the in-flight handler finish (or releases the message unchanged once
  the new `shutdown_timeout` — default 30 s — expires), restores the previous signal handlers and
  closes the transport. Handlers are installed only on the main thread (`handle_signals=False`
  opts out); a second signal forces an immediate exit with the message released. A plain
  `KeyboardInterrupt` keeps its previous behaviour. New `BabelQueue.stop()` ends the loop from any
  thread after the in-flight message.
- `Redeliverer` protocol (`redeliver(message, body, delay)`) — a transport's native
  release; the runtime uses it for retries, unknown-URN `RELEASE` and shutdown releases, and
  falls back to publish + ack otherwise. New `retry_backoff` / `unknown_urn_release_delay`
  settings feed its delay.
- Conformance runners for the `roundtrip`, `data_shape`, `forbidden_keys` and
  `payload_schema_unicode` sections (none skipped).
- `.github/dependabot.yml` (pip + GitHub Actions, weekly).

### Changed
- **SQS release** (broker-bindings §3.5): retries, unknown-URN `RELEASE` and shutdown releases
  **always** call `ChangeMessageVisibility` instead of re-sending, so the broker's
  `ApproximateReceiveCount` carries the attempt count. There is no send + delete fallback. The
  default delay for `retry_backoff` and `unknown_urn_release_delay` is **0 s** (immediate
  redelivery); a delay outside 0..43200 s (including `inf`/`nan`) is clamped with a warning on the
  `babelqueue.sqs` logger. A message without a receipt handle is not released (warning); one
  without a receive count is released but its attempts cannot advance (warning).
  **Poison-loop risk:** a message that always fails is redelivered immediately until
  `max_attempts` — configure a native SQS `RedrivePolicy` (`maxReceiveCount` ≥ `max_attempts`) as
  the backstop and a non-zero `retry_backoff` where needed. A shutdown release also counts as a
  receive on SQS, so keep `shutdown_timeout` above the longest handler time.
- Delivery semantics are documented as **at-least-once**: a handler that finishes right as the
  shutdown deadline fires may be released and run again.
- The SQS, Kafka, Pulsar, Azure Service Bus and Artemis transports' header/property projections
  read the envelope without logging a false forbidden-key drop (the body is sent unchanged).

### Fixed
- The five forbidden envelope keys (`timestamp`, `meta.max_retries`, `meta.attempts`,
  `meta.source`, `meta.ts` — message-envelope §10) are dropped on decode with a warning on the
  `babelqueue.codec` logger, and never written on encode. **Transitional:** encode currently drops
  them with a warning; a future MINOR (K-15 / R1-E0) will reject them instead. A raw-body release
  republish now strips them too (a clean body is re-sent byte for byte); DLQ redrive still restores
  the original bytes unchanged.
- `BabelQueue.stop()` called before `consume()` starts is now honoured instead of being cleared.
- A failed acknowledgement of an already-processed message (e.g. SQS `DeleteMessage` after a
  successful handler, an unknown-URN `DELETE`, or a dead-letter publish) is no longer treated as a
  handler failure: the message is **not** released or retried; the failure is logged at `ERROR`
  on the new `babelqueue.app` logger and the broker redelivers it on its own (SQS: after the
  visibility timeout). The consume loop keeps running.

## [1.13.0] - 2026-06-21

### Added
- **Runtime GDPR field encryption** (ADR-0030) — the optional `babelqueue.gdpr` module ports the Go
  reference to Python: the SDK-enforcement half of governed PII. The babelqueue-registry only
  **declares** and **audits** `x-gdpr-sensitive` fields (and can **mask** them for safe logging);
  this module **enforces** them on the wire — a producer encrypts each marked leaf before publish, a
  consumer decrypts it after decode. Strictly **opt-in**: a producer/consumer that never calls
  `protect`/`unprotect` is unchanged. The **envelope stays frozen** (GR-1): only **values inside
  `data`** change — a sensitive leaf's value becomes a ciphertext **string** — so `data` stays pure
  JSON (an SDK without the key still carries the envelope), `meta.schema_version` stays `1`, and
  `trace_id` is untouched (GR-4). The core stays **stdlib-only** (GR-7): Python's standard library
  has **no AES-GCM**, so the module ships **no** concrete cipher and pulls **no** crypto dependency
  — `Cipher` is a caller-provided `typing.Protocol` (`encrypt(bytes) -> str` / `decrypt(str) ->
  bytes`) bound to a KMS / Vault / HSM / tokenisation service, or to a local AES-256-GCM via the
  optional `cryptography` library (new `babelqueue[gdpr]` extra, documented in the README — still
  **not** a core dependency). `protect(data, schema, cipher)` canonically JSON-encodes each marked
  leaf and replaces it with the cipher's ciphertext string **in place**; `unprotect(...)` is the
  exact inverse, restoring `data` **byte-for-byte**. The marked paths come from the **same per-URN
  schema** the validator already loads — `babelqueue.schema.sensitive_paths` walks nested objects
  (`profile.full_name`), array items (`addresses[].line`), container, and root marks, and the
  `x-gdpr-sensitive` keyword is **validation-neutral** (annotating a schema is never breaking). An
  absent marked field is skipped (not an error); a re-run `unprotect` on already-cleartext data is a
  no-op (non-string leaves are left alone); a wrong key / tampered / non-ciphertext value raises the
  new `babelqueue.DecryptError` so the consumer fails the message (retry / dead-letter) rather than
  handle unreadable PII. Validate cleartext **before** `protect` / **after** `unprotect`, since a
  schema constraining a sensitive field would reject the ciphertext string. Unit-tested without any
  crypto dependency (a stdlib-only authenticated fake cipher) for round-trip exactness across
  nested/array/container/root marks, skips, idempotent re-runs, frozen-envelope decode/validate, and
  wrong-key failure. The envelope is unchanged (`schema_version: 1`); this is purely additive.

## [1.12.0] - 2026-06-21

### Added
- **Transactional outbox helper** (ADR-0029) — the optional `babelqueue.outbox` module ports the
  PHP `BabelQueue\Outbox` helper to Python, removing the producer **dual write**: the message is
  persisted **into your database, in the same transaction** as the business data (so it commits or
  rolls back atomically with it), and a separate **relay** publishes the durable rows afterwards.
  No distributed transaction; exactly-once *handoff* into the broker, then at-least-once on the wire
  (the consumer dedupes on `meta.id` via the idempotency helper, the consumer-side mirror, ADR-0022).
  The core stays **stdlib-only** (GR-7): `OutboxStore` is an abstract `typing.Protocol` the caller
  binds to their own DB — the module ships only the in-memory `InMemoryOutboxStore` reference and
  pulls in **no** DB driver. `Outbox.write(envelope)` encodes via the frozen `EnvelopeCodec` and
  delegates to `OutboxStore.save` **inside the transaction the caller already opened** — it does not
  begin/commit anything (the caller owns the transaction boundary). `OutboxRelay.flush()` publishes
  one batch through the existing publish-only `Transport`, marking each row published **only after**
  the transport accepts it, or failed (caught → `mark_failed`, row left pending, with a bounded
  linear backoff via an injectable sleeper) so one poison row never blocks the batch;
  `OutboxRelay.drain()` loops while a pass makes progress, with a safety ceiling. The relay
  publishes the **stored bytes verbatim** — it never decodes, rebuilds or re-encodes the envelope —
  so `trace_id` is preserved end-to-end and the body is byte-compatible across SDKs (GR-1/GR-4/GR-5).
  Unit-tested without a broker (write stores the encoded envelope byte-identical; relay publishes via
  a fake `Transport` + marks published; a raising publish → `mark_failed`, row still pending, batch
  continues; `drain` loops to empty and stops on no-progress; backoff grows linearly and caps via the
  injected sleeper). The envelope is unchanged (`schema_version: 1`); this is purely additive.

## [1.11.0] - 2026-06-21

### Added
- **W3C `traceparent` span-context propagation** (OpenTelemetry v0.2, ADR-0028) — the optional
  `babelqueue.otel` module now carries true cross-hop **span** parent-child linkage, not just
  shared-`trace_id` correlation. On publish, `otel.publish` injects the active span context as a
  W3C `traceparent` **transport header** (and still stamps `trace_id` for the v0.1 fallback); on
  consume, `otel.wrap_handler` extracts it and starts the CONSUMER span as a true **child** of the
  producer span. With no `traceparent` present it falls back to the v0.1 `trace_id`-derived parent,
  so it is a strict, backward-compatible upgrade — no regression. The header rides **out of band**
  via a new dependency-free core seam — `BabelQueue.publish_with_headers(urn, data, headers, …)`
  (produce side) and `babelqueue.headers_from_context()` (consume side, surfaced by the runtime) —
  so the wire envelope stays **frozen** (`schema_version: 1`, GR-1) and the core stays
  zero-dependency (OTel remains the optional `[otel]` extra, GR-7). `traceparent` is carried on the
  **in-memory** (reference), **Redis** (a transport-owned `__bq_frame` JSON frame with bare-value
  back-compat, so cross-version queues interoperate; degrades to a bare publish in Laravel-compat
  mode), **RabbitMQ** (native AMQP header table, beside the contract `x-*` headers) and **SQS**
  (native `MessageAttributes`, beside the contract `bq-*` attributes) transports; where a transport
  can't carry headers, propagation degrades cleanly to v0.1 `trace_id` correlation with no error.
  A plain `publish` is byte-identical to before. Unit-tested without a broker (frame round-trip +
  bare back-compat, header merge/extract per transport, and an in-memory producer→consumer
  parent-child end-to-end with the OTel SDK's `InMemorySpanExporter`); broker-gated integration
  tests assert a published `traceparent` arrives on the consumed message's headers beside the
  unchanged body. The envelope is unchanged; this is purely additive. Ships as a MINOR.

## [1.6.0] - 2026-06-14

### Added
- **Redis/Laravel reservation parity** — the Redis transport can now consume a
  **shared** Laravel BabelQueue Redis queue using Laravel's reserved-set / reliable-queue
  semantics, not just a Python-owned queue. Enable it with the `laravel=1` URL flag
  (`redis://host:6379/0?laravel=1`) or `RedisTransport(..., laravel_compat=True)`. In this
  mode the key layout is Laravel's stock Redis queue ([§1 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#redis)): a `queues:<name>`
  ready list, a `queues:<name>:reserved` **sorted set** scored by a `retry_after` deadline
  (default 60s), a `queues:<name>:delayed` set, and a `queues:<name>:notify` wake-up list.
  Reserve, ack and release run the **byte-for-byte same Lua scripts** Laravel uses, so the
  reserved member a Python worker writes is identical to a Laravel worker's — either side can
  ack (`ZREM`) the other's reservation, so a Python worker and a Laravel worker share one
  Redis queue without losing or double-processing messages. Before each pop, expired
  reserved/delayed jobs migrate back to the ready list, so a crashed worker's in-flight job
  is re-reserved exactly as Laravel does. Tunable via `?prefix=` / `?retry_after=` (or the
  `key_prefix` / `retry_after` constructor args). The default Python-owned reliable-queue
  mode (`BLMOVE` + `<queue>:processing` list) is unchanged and stays the default, so existing
  callers are unaffected. The reservation logic is fully unit-tested with an injected
  in-memory Redis double (no `redis` package, no broker); a live cross-runtime PHP↔Python
  shared-queue round-trip is covered by the integration suite where a real Redis is present.
  The envelope is unchanged (`schema_version: 1`); this is purely additive. Ships as a MINOR.

## [1.5.0] - 2026-06-13

### Added
- **Apache ActiveMQ Artemis transport** (`babelqueue[artemis]`, `python-qpid-proton`) —
  `ArtemisTransport`, selected by the `artemis://` (or `artemis+ssl://`) URL scheme (e.g.
  `artemis://localhost:5672`; or pass an injected `connection`). Artemis speaks **AMQP 1.0**
  (not RabbitMQ's 0-9-1), so the transport uses the `python-qpid-proton` blocking client.
  Implements [§7 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis): the
  canonical envelope is the message body, projected onto the AMQP fields a JMS peer reads —
  `correlation-id` = `trace_id` (JMSCorrelationID), `creation-time` = `meta.created_at`
  (JMSTimestamp), the `x-opt-jms-type` annotation = URN (JMSType, the AMQP-JMS mapping), plus
  the `bq-schema-version`/`bq-source-lang`/`bq-attempts`/`bq-app-id` application properties.
  Consume reserves one message at a time (`receive` → process → `accept`); `attempts` is
  reconciled to `max(body, delivery_count)` — the AMQP delivery-count header is 0-based, so it
  maps directly with no −1 (the Java JMS binding reads the 1-based `JMSXDeliveryCount` and
  subtracts 1, arriving at the same 0-based `attempts`), and the `max` never lowers a higher
  body count carried by a republish-driven retry. The projection + reconciliation + pop/ack
  flow are unit-tested with no broker and no `python-qpid-proton` (the proton import is lazy;
  the transport talks to an injected connection fake); the publish flow that builds a real
  proton `Message` is exercised wherever proton is installed. The envelope is unchanged
  (`schema_version: 1`); Apache ActiveMQ Artemis is purely additive. Ships as a MINOR.

## [1.4.0] - 2026-06-13

### Added
- **Apache Kafka transport** (`babelqueue[kafka]`, `confluent-kafka`) — `KafkaTransport`,
  selected by the `kafka://` URL scheme (e.g. `kafka://host:9092`; or pass an injected
  `producer` + `consumer_factory`). Implements [§6 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka): the record
  **value** is the canonical envelope, projected onto native Kafka record headers (UTF-8 byte
  strings) — `bq-job` = URN, `bq-trace-id`, `bq-message-id`, plus `bq-schema-version`/
  `bq-source-lang`/`bq-attempts` — with the record timestamp mirroring `meta.created_at`.
  Consume is **process-then-commit** (`pop` reserves via `poll` with `enable.auto.commit=false`,
  `ack` commits the offset); the **`bq-attempts` header is the authoritative attempt counter**
  (the body's `attempts` is the fallback for non-BabelQueue producers). The projection +
  reconciliation + publish/pop/ack flow are unit-tested with no broker and no `confluent-kafka`
  (the kafka import is lazy; the transport talks to injected producer/consumer fakes). The
  envelope is unchanged (`schema_version: 1`); Apache Kafka is purely additive. Ships as a MINOR.

## [1.3.0] - 2026-06-13

### Added
- **Apache Pulsar transport** (`babelqueue[pulsar]`, `pulsar-client`) — `PulsarTransport`,
  selected by the `pulsar://` (or `pulsar+ssl://`) URL scheme (e.g.
  `pulsar://localhost:6650`; or pass a built `client`). Implements [§5 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar): the canonical
  envelope is the message payload, projected onto native Pulsar message properties
  (string→string) — `bq-job` = URN, `bq-trace-id` = `trace_id`, `bq-message-id` = `meta.id`,
  plus `bq-schema-version`/`bq-source-lang`/`bq-attempts`; receive → `acknowledge`; `attempts`
  reconciled to `max(bq-attempts, redelivery_count)` (Pulsar's redelivery count is 0-based, so
  it maps directly with no −1, and the `max` never lowers a higher body count carried by a
  republish-driven retry). Default `Shared` subscription named `babelqueue`; a client can be
  injected for tests/DI. The projection + reconciliation + publish/pop/ack flow are unit-tested
  with no broker and no `pulsar-client` (the pulsar import is lazy and publishing sends raw
  bytes). The envelope is unchanged (`schema_version: 1`); Apache Pulsar is purely additive.
  Ships as a MINOR.

## [1.2.0] - 2026-06-13

### Added
- **Azure Service Bus transport** (`babelqueue[azureservicebus]`, `azure-servicebus`) —
  `AsbTransport`, selected by the `sb://` URL scheme (e.g.
  `sb://<namespace>.servicebus.windows.net`, Azure AD via `DefaultAzureCredential`; or pass
  `connection_string=...` / a built `client`). Implements [§4 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#azure-service-bus): the
  canonical envelope is the message body, projected onto native Service Bus fields
  (`Subject` = URN, `CorrelationId` = `trace_id`, `MessageId` = `meta.id`, plus the `bq-`
  application properties); PeekLock reserve → `complete_message` ack; `attempts` reconciled
  to the broker-authoritative `DeliveryCount − 1`. A client can be injected for tests/DI. The
  projection + reconciliation are unit-tested with no broker and no `azure-servicebus` (the
  azure import is lazy); the publish flow is covered with a fake client. The envelope is
  unchanged (`schema_version: 1`); Azure Service Bus is purely additive. Ships as a MINOR.

## [1.1.0] - 2026-06-12

### Added
- **Amazon SQS transport** (`babelqueue[sqs]`, `boto3`) — `SqsTransport`, selected by
  the `sqs://` URL scheme (e.g. `sqs://us-east-1?endpoint=http://localhost:4566` for
  LocalStack). Implements [§3 of the broker-bindings contract](https://babelqueue.com):
  the canonical envelope is the `MessageBody`, projected onto native `MessageAttributes`
  (`bq-job`/`bq-trace-id`/`bq-message-id`/`bq-schema-version`/`bq-source-lang`/`bq-created-at`);
  visibility-timeout reserve → `delete_message` ack; `attempts` reconciled to
  `ApproximateReceiveCount − 1` (never lowering a runtime-incremented count). Supports
  FIFO (`MessageGroupId`/`MessageDeduplicationId` = `meta.id`), content-based dedup,
  configurable wait/visibility, and a `queue_url_prefix` that skips `GetQueueUrl`. A
  client can be injected for tests/DI. 16 unit tests run against a fake client (no boto3,
  no broker); a LocalStack integration test round-trips the real `boto3` path. The
  envelope is unchanged (`schema_version: 1`); SQS is purely additive. Ships as a MINOR.

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable**: breaking changes require a MAJOR,
following the deprecation policy. The wire envelope is unchanged
(`schema_version: 1`); the core + Celery/Django adapters ship together. Full
reference at [babelqueue.com](https://babelqueue.com).

### Internal
- CI adds **ruff** + **mypy** static analysis and a **>=90% coverage gate**
  (`pytest --cov --cov-fail-under=90`, run in the broker-backed job so the Redis /
  RabbitMQ transports count). Type-safety fix in `redis_transport` (str-narrow the
  BLMOVE reply) surfaced by mypy — no behaviour change.
- **GR-8 latency benchmark** (`tests/test_overhead.py`) — asserts the envelope
  encode/decode path adds **≤2%** over plain-JSON serialization vs a conservative
  2ms broker round-trip (the pure-Python codec is slower than the compiled SDKs —
  ~16µs marginal on CPython 3.9/CI — so the reference is higher to stay robust).

## [0.5.0] - 2026-06-06

### Added
- **Celery adapter** (`babelqueue.celery`, `[celery]` extra) — `from_celery(app)`
  builds a `BabelQueue` runtime on a Celery app's broker, and `install_worker(app)`
  registers a Celery worker bootstep that drains URN-routed polyglot messages in a
  background thread alongside Celery's own consumer.
- **Django adapter** (`babelqueue.django`, `[django]` extra) — settings-driven
  `BABELQUEUE` config, `get_app()` / `publish()` shortcuts, and a
  `manage.py babelqueue_worker` management command. Add `"babelqueue.django"` to
  `INSTALLED_APPS`.
- Both adapters lazy-import their framework, so the core stays dependency-free.

## [0.4.0] - 2026-06-06

### Added
- `EnvelopeCodec.urn()` — resolve the URN (`job`, accepting `urn` as an alias).
- `EnvelopeCodec.accepts()` — consumer-side envelope validation (rejects empty URN,
  unsupported `meta.schema_version`, blank `trace_id`, non-object `data`).
- Shared **cross-SDK conformance suite** under `tests/conformance/` (vendored from
  the canonical `conformance/` set) plus a `test_conformance.py` runner.

## [0.3.0] - 2026-06-06

### Added
- **RabbitMQ transport** (`PikaTransport`, `amqp://`): durable queue, persistent
  delivery, `basic_get` + manual ack, and the contract AMQP properties (`type`=URN,
  `correlation_id`=trace_id, `x-schema-version`/`x-source-lang`/`x-attempts`).
  Optional `[amqp]` extra (lazy `pika` import) — the core stays zero-dep.

## [0.2.0] - 2026-06-06

### Added
- **Runtime** — `BabelQueue(broker_url=...)` app with a `@app.handler("urn:...")`
  decorator, `publish()`, and a `consume()` / `run()` loop. Routes by URN over the
  canonical envelope; `attempts`-based retry → opt-in dead-letter queue;
  `on_unknown_urn` strategies (`fail`/`delete`/`release`/`dead_letter`).
- **Transports** — a pluggable `Transport` abstraction with `InMemoryTransport`
  (`memory://`, for tests/local) and `RedisTransport` (`redis://`, reliable-queue
  pattern via `BLMOVE` + a processing list). Redis client is an optional `[redis]`
  extra, imported lazily — the core stays zero-dep.

## [0.1.0] - 2026-06-06

### Added
- `EnvelopeCodec` — builds (`make`, `from_message`), encodes and decodes the
  canonical `{job, trace_id, data, meta, attempts}` envelope (`schema_version` 1).
  The single Python implementation of the wire format.
- Contracts `PolyglotMessage` / `HasTraceId` (typed `Protocol`s).
- `dead_letter.annotate()` — additive `dead_letter` block builder.
- `UnknownUrnStrategy` — `fail` / `delete` / `release` / `dead_letter`.
- `BabelQueueError` / `UnknownUrnError`.
- Golden conformance fixtures under `tests/fixtures/` (shared cross-SDK set).
- `py.typed` — ships inline type hints (PEP 561).

### Notes
- Pre-1.0: the public API may change before the `1.0.0` tag.
- The core has **zero runtime dependencies** (standard library only); Python `>=3.9`.

[Unreleased]: https://github.com/BabelQueue/babelqueue-python/compare/v1.6.0...HEAD
[1.6.0]: https://github.com/BabelQueue/babelqueue-python/compare/v1.5.0...v1.6.0
[1.2.0]: https://github.com/BabelQueue/babelqueue-python/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/BabelQueue/babelqueue-python/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.5.0...v1.0.0
[0.5.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/babelqueue-python/releases/tag/v0.1.0
