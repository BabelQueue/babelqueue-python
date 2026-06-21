# BabelQueue for Python

[![CI](https://github.com/BabelQueue/babelqueue-python/actions/workflows/ci.yml/badge.svg)](https://github.com/BabelQueue/babelqueue-python/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/babelqueue.svg)](https://pypi.org/project/babelqueue/)
[![Python](https://img.shields.io/pypi/pyversions/babelqueue.svg)](https://pypi.org/project/babelqueue/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Polyglot Queues, Simplified.** Read and write the canonical BabelQueue message
> envelope from Python — so your Python services (AI/ML, data processing, …)
> exchange messages with Laravel, Symfony, Go, .NET and Node over one strict JSON
> format, on the broker you already run.

This is the framework-agnostic **Python core**: the wire-envelope codec,
contracts, and dead-letter helpers — **zero runtime dependencies** (standard
library only). The full standard is documented at
**[babelqueue.com](https://babelqueue.com)**.

## Installation

```bash
pip install babelqueue
```

Requires Python `>=3.9`.

## Usage

```python
from babelqueue import EnvelopeCodec

# Produce — build the canonical envelope and publish the JSON to your broker.
envelope = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042})
body = EnvelopeCodec.encode(envelope)        # -> UTF-8 JSON string
# redis.rpush("queues:orders", body)  /  channel.basic_publish(body=body, ...)

# Consume — decode a message produced by ANY BabelQueue SDK.
incoming = EnvelopeCodec.decode(body)
urn      = incoming["job"]          # "urn:babel:orders:created"
data     = incoming["data"]         # {"order_id": 1042}
trace_id = incoming["trace_id"]     # correlate across services
```

The envelope is identical to every other SDK's:

```json
{
  "job": "urn:babel:orders:created",
  "trace_id": "…",
  "data": { "order_id": 1042 },
  "meta": { "id": "…", "queue": "default", "lang": "python", "schema_version": 1, "created_at": 1749132727000 },
  "attempts": 0
}
```

### Typed messages (optional)

```python
from babelqueue import EnvelopeCodec, PolyglotMessage

class OrderCreated:                      # structurally a PolyglotMessage
    def __init__(self, order_id: int):
        self.order_id = order_id
    def get_babel_urn(self) -> str:
        return "urn:babel:orders:created"
    def to_payload(self) -> dict:
        return {"order_id": self.order_id}

envelope = EnvelopeCodec.from_message(OrderCreated(1042), queue="orders")
```

Continue an existing trace by adding `get_babel_trace_id(self) -> str | None`
(see `HasTraceId`), or pass `trace_id=` to `EnvelopeCodec.make`.

### Dead-letter

```python
from babelqueue import dead_letter

dlq = dead_letter.annotate(envelope, "failed", "orders", attempts=3, error="boom")
# publish `EnvelopeCodec.encode(dlq)` to the "orders.dlq" queue
```

## Runtime — produce & consume

For an end-to-end app, use `BabelQueue` with a broker. Broker clients come via
extras:

```bash
pip install "babelqueue[redis]"   # redis://
pip install "babelqueue[amqp]"    # amqp:// (RabbitMQ)
```

```python
from babelqueue import BabelQueue

app = BabelQueue("redis://localhost:6379/0", queue="orders")
# or: BabelQueue("amqp://guest:guest@localhost:5672/", queue="orders")

@app.handler("urn:babel:orders:created")
def on_order_created(data, meta):       # AI/ML, data processing, anything
    print("order", data["order_id"])

# producer (any service, any language) …
app.publish("urn:babel:orders:created", {"order_id": 1042})

# worker
app.run()                               # consume forever (Ctrl-C to stop)
```

- **Routing** is by URN; the wire format is the canonical envelope, so this
  consumes messages produced by *any* BabelQueue SDK.
- **Handlers** receive `(data, meta)`, or `(data, meta, message)` to get the full
  envelope (incl. `trace_id`).
- **Retry & dead-letter:** failures are retried up to `max_attempts` (bumping the
  envelope's `attempts`); enable `dead_letter=True` to quarantine exhausted
  messages on `<queue>.dlq`. `on_unknown_urn` = `fail` | `delete` | `release` | `dead_letter`.
- **Transports:** `redis://` (reliable-queue pattern; add `?laravel=1` to share a
  Redis queue with a Laravel BabelQueue worker using its reserved-set semantics —
  see below), `amqp://` (RabbitMQ via `pika`, with the contract AMQP properties)
  and `memory://` (in-process, great for tests/local). Bring your own by passing
  `transport=...`.

### Sharing a Redis queue with Laravel

By default the Redis transport owns its queue end-to-end (`RPUSH` to produce;
`BLMOVE` into a `<queue>:processing` list to reserve; `LREM` on ack). To consume a
**shared** Laravel BabelQueue Redis queue instead, enable Laravel-compatible mode:

```python
app = BabelQueue("redis://localhost:6379/0?laravel=1", queue="orders")
# or: RedisTransport("redis://localhost:6379/0", laravel_compat=True)
```

This replicates Laravel's stock Redis reservation exactly ([§1 of the
broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#redis)):
the ready list is `queues:<name>`, reservations move into the `queues:<name>:reserved`
**sorted set** scored by a `retry_after` deadline (default 60s), with a
`queues:<name>:delayed` set and a `queues:<name>:notify` wake-up list. Reserve, ack
and release run the **byte-for-byte same Lua scripts** Laravel uses, so the reserved
member a Python worker writes is identical to a Laravel worker's — either side can
ack (`ZREM`) the other's reservation, so a Python worker and a Laravel worker share
one queue without losing or double-processing messages. Before each pop, expired
reserved/delayed jobs migrate back to the ready list, so a crashed worker's
in-flight job is re-reserved. Options: `?prefix=queues:` and `?retry_after=60`
(or the `key_prefix` / `retry_after` constructor args).

## Framework adapters — Celery & Django

**Celery** (`pip install "babelqueue[celery]"`) — reuse your Celery app's broker for
polyglot interop, and consume inbound messages as a Celery worker bootstep:

```python
from babelqueue.celery import from_celery, install_worker

bq = from_celery(celery_app, queue="orders")    # runtime on Celery's broker
bq.publish("urn:babel:orders:created", {"order_id": 1042})

@bq.handler("urn:babel:orders:created")
def on_created(data, meta): ...

install_worker(celery_app, bq)                   # `celery worker` also drains URN messages
```

**Django** (`pip install "babelqueue[django]"`) — add `"babelqueue.django"` to
`INSTALLED_APPS` and configure a `BABELQUEUE` dict:

```python
# settings.py
BABELQUEUE = {"broker_url": "redis://localhost:6379/0", "queue": "orders", "dead_letter": True}
```

```python
from babelqueue.django import publish, get_app

publish("urn:babel:orders:created", {"order_id": 1042})   # in a view / signal

@get_app().handler("urn:babel:orders:created")            # register handlers at startup
def on_created(data, meta): ...
```

```bash
python manage.py babelqueue_worker --queue orders          # run the consumer
```

## Transactional outbox (optional)

The `babelqueue.outbox` helper (ADR-0029) removes the producer **dual write**: "commit the
business row" and "publish to the broker" are two systems that can disagree on a crash. Instead the
message is persisted **into your database, in the same transaction** as the business data — so it
commits or rolls back atomically with it — and a separate **relay** publishes the durable rows
afterwards. No distributed transaction; exactly-once *handoff* into the broker, then at-least-once
on the wire (the consumer dedupes on `meta.id` — see the idempotency helper, the mirror of this).

The core stays **stdlib-only**: `OutboxStore` is an abstract `Protocol` you bind to **your own DB**
(the core ships no driver). The stored value is the `EnvelopeCodec`-encoded envelope **byte-for-byte
unchanged** (frozen, `schema_version: 1`); the relay publishes those exact bytes — it never decodes,
rebuilds or re-encodes — so `trace_id` is preserved end-to-end.

```python
from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.outbox import Outbox, OutboxRelay, InMemoryOutboxStore

store = InMemoryOutboxStore()          # production: your own OutboxStore adapter, DB-backed
outbox = Outbox(store)

# write side — YOU own the transaction boundary (this is the whole point):
with db.transaction():                 # your own open transaction
    db.insert_order(order)             # the business write
    envelope = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042}, queue="orders")
    outbox.write(envelope)             # same connection, same tx — both, or neither

# read/publish side — run on a short interval, after the business tx commits:
app = BabelQueue("redis://localhost:6379/0", queue="orders")
relay = OutboxRelay(app.transport, store)
relay.drain()                          # publish all pending rows; flush() does one batch
```

`Outbox.write` only encodes and calls `OutboxStore.save` — it does **not** begin or commit anything.
A `save` runs inside the transaction you already opened; you commit both together. `OutboxRelay`
marks a row published only **after** the transport accepts it; a publish that raises is recorded via
`mark_failed` (with a bounded, injectable-sleeper backoff) and left pending for a later pass, so one
poison row never blocks the batch. Implement `OutboxStore` over your DB (claim rows oldest-first,
ideally with `SELECT … FOR UPDATE SKIP LOCKED` so two relays don't double-publish); `InMemoryOutboxStore`
is the reference for tests and single-process demos (no real transaction).

## OpenTelemetry tracing (optional)

`pip install "babelqueue[otel]"` adds the optional `babelqueue.otel` module — the core never
imports OpenTelemetry, so it stays zero-dependency. It emits a PRODUCER span per publish and a
CONSUMER span per handled message, correlated across every hop and SDK, at two layered levels:

- **`trace_id` correlation** (v0.1): the envelope's `trace_id` maps 1:1 to an OTel trace id, so
  every hop that shares a `trace_id` shares one trace — with **zero** wire/transport change.
- **W3C `traceparent` span linkage** (v0.2): the producer also injects its active span context as
  a `traceparent` **transport header** (beside the frozen envelope, never in it), so the consumer
  starts its span as a true **child** of the producer span — real cross-hop parent-child linkage.
  With no `traceparent` present it falls back to the v0.1 `trace_id` behaviour, so enabling it is a
  strict, backward-compatible upgrade.

```python
from opentelemetry import trace
from babelqueue import BabelQueue, otel

tracer = trace.get_tracer("orders")
app = BabelQueue("redis://localhost:6379/0", queue="orders")

# consumer: wrap_handler starts a CONSUMER span (child of the producer span when a
# traceparent rode along; else in the trace_id-derived trace)
app.register("urn:babel:orders:created", otel.wrap_handler(tracer, on_order_created))

# producer: otel.publish starts a PRODUCER span and carries traceparent + trace_id
otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 1042})
```

The `traceparent` rides the out-of-band transport-header seam (`publish_with_headers` /
`headers_from_context`) — the same seam the replay-bypass marker uses — so the envelope stays
frozen (`schema_version: 1`). It is carried on the in-memory, Redis (a transport-owned JSON frame,
with bare-value back-compat), RabbitMQ (AMQP header table) and SQS (`MessageAttributes`)
transports; where a transport can't carry it, propagation degrades cleanly to v0.1 `trace_id`
correlation with no error.

## What's here

The codec/contracts/dead-letter (zero-dep core), the `BabelQueue` runtime
(in-memory built in; Redis via `[redis]`, RabbitMQ via `[amqp]`), and framework
adapters for **Celery** (`[celery]`) and **Django** (`[django]`). Every layer
speaks the one canonical envelope, so it interoperates with the PHP/Laravel,
Symfony, Go, Node and .NET SDKs.

## Testing

```bash
pip install -e ".[dev]"
pytest
# (or, dependency-free) python -m unittest discover -s tests
```

## License

MIT © Muhammet Şafak. See [LICENSE](LICENSE).
