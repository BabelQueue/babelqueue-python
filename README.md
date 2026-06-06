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

> **Celery** / **Django** adapters are the next iterations.

## What's here

The codec/contracts/dead-letter (zero-dep core) **and** the `BabelQueue` runtime
above (in-memory built in; Redis via `[redis]`, RabbitMQ via `[amqp]`). For
framework integration, the Celery and Django adapters are planned.

## Testing

```bash
pip install -e ".[dev]"
pytest
# (or, dependency-free) python -m unittest discover -s tests
```

## License

MIT © Muhammet Şafak. See [LICENSE](LICENSE).
