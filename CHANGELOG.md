# Changelog

All notable changes to `babelqueue` (Python) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **Runtime** — `BabelQueue(broker_url=...)` app with a `@app.handler("urn:...")`
  decorator, `publish()`, and a `consume()` / `run()` loop. Routes by URN over the
  canonical envelope; `attempts`-based retry → opt-in dead-letter queue;
  `on_unknown_urn` strategies (`fail`/`delete`/`release`/`dead_letter`).
- **Transports** — a pluggable `Transport` abstraction with `InMemoryTransport`
  (`memory://`, for tests/local) and `RedisTransport` (`redis://`, reliable-queue
  pattern via `BLMOVE` + a processing list). Redis client is an optional extra
  (`pip install "babelqueue[redis]"`), imported lazily — the core stays zero-dep.

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-python/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/BabelQueue/babelqueue-python/releases/tag/v0.1.0
