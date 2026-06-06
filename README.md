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
- **Transports:** `redis://` (reliable-queue pattern), `amqp://` (RabbitMQ via
  `pika`, with the contract AMQP properties) and `memory://` (in-process, great for
  tests/local). Bring your own by passing `transport=...`.

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
