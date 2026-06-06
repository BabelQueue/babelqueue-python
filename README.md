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

## What's here vs. coming

- **Now (this package):** the codec, contracts, dead-letter and unknown-URN
  helpers, plus the shared conformance fixtures. Bring your own broker client.
- **Next (planned):** a built-in runtime — `BabelQueue(broker_url=...)` with an
  `@app.handler("urn:…")` decorator over `redis`/`pika` — and **Celery** / **Django**
  adapters. Install via extras (`babelqueue[redis]`, `babelqueue[celery]`, …).

## Testing

```bash
pip install -e ".[dev]"
pytest
# (or, dependency-free) python -m unittest discover -s tests
```

## License

MIT © Muhammet Şafak. See [LICENSE](LICENSE).
