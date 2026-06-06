# Changelog

All notable changes to `babelqueue` (Python) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-python/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/BabelQueue/babelqueue-python/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BabelQueue/babelqueue-python/releases/tag/v0.1.0
