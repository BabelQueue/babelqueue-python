# Changelog

All notable changes to `babelqueue` (Python) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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
