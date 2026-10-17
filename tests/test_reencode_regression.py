"""Regression: every path that re-encodes a *raw* inbound body must go through
``EnvelopeCodec.decode`` first (ADR-0031 (c)). ``encode`` rejects forbidden keys and a top-level
``urn`` alias, so ``parse_envelope`` -> ``encode`` would raise on a legacy body and strand the
message. Covers the SQS, Pulsar, Kafka and Azure Service Bus attempts reconciliation and the
runtime's raw-body RELEASE re-publish, for: a legacy ``timestamp``, a ``urn``-only body, and
unknown top-level / ``meta`` keys that must survive."""

from __future__ import annotations

import json
import unittest
from typing import Any, Callable, Dict, Optional

from babelqueue import BabelQueue, EnvelopeCodec, InMemoryTransport, UnknownUrnStrategy
from babelqueue.asb_transport import AsbTransport
from babelqueue.kafka_transport import KafkaTransport
from babelqueue.pulsar_transport import PulsarTransport
from babelqueue.sqs_transport import SqsTransport

URN = "urn:babel:orders:created"


def _base() -> Dict[str, Any]:
    return {
        "job": URN,
        "trace_id": "trace-1",
        "data": {"a": 1},
        "meta": {
            "id": "m-1",
            "queue": "orders",
            "lang": "php",
            "schema_version": 1,
            "created_at": 1700000000000,
            "ext": {"acme": {"tenant": "t1"}},
            "x_unknown_meta": "kept",
        },
        "attempts": 0,
        "x_unknown_top": {"k": [1, 2]},
    }


def _legacy_timestamp() -> str:
    raw = _base()
    raw["timestamp"] = 1700000000
    return json.dumps(raw)


def _urn_only() -> str:
    raw = _base()
    raw["urn"] = raw.pop("job")
    return json.dumps(raw)


def _timestamp_and_urn_only() -> str:
    raw = _base()
    raw["urn"] = raw.pop("job")
    raw["timestamp"] = 1700000000
    return json.dumps(raw)


class _Mixin:
    """``reconcile(body)`` must return a body whose attempts is 3."""

    reconcile: Callable[[str], str]

    def assertKept(self, out: str) -> None:
        env = json.loads(out)
        self.assertEqual(env["attempts"], 3)  # type: ignore[attr-defined]
        self.assertEqual(env["job"], URN)  # type: ignore[attr-defined]
        self.assertNotIn("urn", env)  # type: ignore[attr-defined]
        self.assertNotIn("timestamp", env)  # type: ignore[attr-defined]
        self.assertEqual(env["x_unknown_top"], {"k": [1, 2]})  # type: ignore[attr-defined]
        self.assertEqual(env["meta"]["ext"], {"acme": {"tenant": "t1"}})  # type: ignore[attr-defined]
        self.assertEqual(env["meta"]["x_unknown_meta"], "kept")  # type: ignore[attr-defined]
        self.assertEqual(env["trace_id"], "trace-1")  # type: ignore[attr-defined]

    def test_legacy_timestamp_body(self) -> None:
        with self.assertLogs("babelqueue.codec", level="WARNING"):  # type: ignore[attr-defined]
            self.assertKept(type(self).reconcile(_legacy_timestamp()))

    def test_urn_only_body(self) -> None:
        self.assertKept(type(self).reconcile(_urn_only()))

    def test_timestamp_and_urn_only_body(self) -> None:
        with self.assertLogs("babelqueue.codec", level="WARNING"):  # type: ignore[attr-defined]
            self.assertKept(type(self).reconcile(_timestamp_and_urn_only()))

    def test_clean_body_unknown_keys_preserved(self) -> None:
        self.assertKept(type(self).reconcile(json.dumps(_base())))


class SqsReencodeTest(_Mixin, unittest.TestCase):
    reconcile = staticmethod(lambda body: SqsTransport._reconcile(body, 4))  # type: ignore[assignment]


class PulsarReencodeTest(_Mixin, unittest.TestCase):
    reconcile = staticmethod(  # type: ignore[assignment]
        lambda body: PulsarTransport._reconcile(body, 3)[0]
    )


class KafkaReencodeTest(_Mixin, unittest.TestCase):
    reconcile = staticmethod(  # type: ignore[assignment]
        lambda body: KafkaTransport._reconcile(body, [("bq-attempts", b"3")])
    )


class AsbReencodeTest(_Mixin, unittest.TestCase):
    reconcile = staticmethod(  # type: ignore[assignment]
        lambda body: AsbTransport._reconcile(body, 4)
    )


class RuntimeRawReleaseTest(unittest.TestCase):
    """Unknown-URN RELEASE on a transport without native release re-publishes the raw body."""

    def _release(self, raw: str) -> Optional[str]:
        transport = InMemoryTransport()
        app = BabelQueue(
            transport=transport, queue="orders", on_unknown_urn=UnknownUrnStrategy.RELEASE
        )
        transport.publish("orders", raw)
        self.assertEqual(app.consume(max_messages=1, timeout=0, handle_signals=False), 1)
        released = list(transport._queues["orders"])  # type: ignore[attr-defined]
        self.assertEqual(len(released), 1)
        return released[0]

    def _unknown(self, mutate: Callable[[Dict[str, Any]], None]) -> str:
        raw = _base()
        raw["job"] = "urn:babel:nobody:listens"
        mutate(raw)
        return json.dumps(raw)

    def test_legacy_timestamp_is_dropped_and_released_with_attempts_unchanged(self) -> None:
        raw = self._unknown(lambda r: r.update(timestamp=1))
        with self.assertLogs("babelqueue.codec", level="WARNING"):
            out = json.loads(self._release(raw) or "")
        self.assertNotIn("timestamp", out)
        self.assertEqual(out["attempts"], 0)
        self.assertEqual(out["x_unknown_top"], {"k": [1, 2]})
        self.assertEqual(out["meta"]["ext"], {"acme": {"tenant": "t1"}})
        self.assertEqual(out["meta"]["x_unknown_meta"], "kept")

    def test_urn_only_body_is_released_without_raising(self) -> None:
        def mutate(r: Dict[str, Any]) -> None:
            r["urn"] = r.pop("job")

        raw = self._unknown(mutate)
        released = self._release(raw)
        self.assertEqual(released, raw)  # no forbidden key -> verbatim

    def test_timestamp_plus_urn_only_body_folds_urn_into_job(self) -> None:
        def mutate(r: Dict[str, Any]) -> None:
            r["urn"] = r.pop("job")
            r["timestamp"] = 1

        raw = self._unknown(mutate)
        with self.assertLogs("babelqueue.codec", level="WARNING"):
            out = json.loads(self._release(raw) or "")
        self.assertEqual(out["job"], "urn:babel:nobody:listens")
        self.assertNotIn("urn", out)
        self.assertNotIn("timestamp", out)
        self.assertEqual(out["attempts"], 0)
        self.assertEqual(out["x_unknown_top"], {"k": [1, 2]})

    def test_reencode_of_decoded_body_round_trips(self) -> None:
        out = EnvelopeCodec.encode(EnvelopeCodec.decode(_timestamp_and_urn_only()))
        self.assertEqual(json.loads(out)["job"], URN)


if __name__ == "__main__":
    unittest.main()
