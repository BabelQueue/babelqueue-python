# Changelog

All notable changes to `babelqueue` (Python) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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
