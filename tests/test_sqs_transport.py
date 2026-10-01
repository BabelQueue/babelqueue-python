"""Tests for the Amazon SQS transport.

The unit tests inject a fake SQS client, so they run without ``boto3`` and without
a broker. A separate integration test round-trips against LocalStack and is skipped
unless ``boto3`` + a reachable endpoint are present (CI runs it).
"""

from __future__ import annotations

import contextlib
import logging
import os
import unittest
import uuid

from babelqueue import BabelQueue, EnvelopeCodec, UnknownUrnStrategy
from babelqueue.sqs_transport import SqsTransport
from babelqueue.transport import HeaderPublisher, ReceivedMessage, Redeliverer, make_transport


class FakeSQS:
    """In-memory SQS API — no boto3, no network."""

    def __init__(self, err: Exception | None = None) -> None:
        self.visible: dict[str, list[dict]] = {}
        self.sent: list[dict] = []
        self.deleted: list[str] = []
        self.visibility_changes: list[dict] = []
        self.inflight: dict[str, tuple[str, dict]] = {}
        self.get_url_calls = 0
        self.last_receive: dict | None = None
        self.err = err
        self._n = 0

    def get_queue_url(self, QueueName):  # noqa: N803 - AWS API casing
        self.get_url_calls += 1
        if self.err:
            raise self.err
        return {"QueueUrl": "http://fake/" + QueueName}

    def send_message(self, **kw):
        if self.err:
            raise self.err
        self.sent.append(kw)
        self._n += 1
        handle = f"rh-{self._n}"
        self.visible.setdefault(kw["QueueUrl"], []).append(
            {
                "Body": kw["MessageBody"],
                "MessageAttributes": kw.get("MessageAttributes"),
                "ReceiptHandle": handle,
                "Attributes": {"ApproximateReceiveCount": "1"},
            }
        )
        return {"MessageId": handle}

    def receive_message(self, **kw):
        self.last_receive = kw
        if self.err:
            raise self.err
        q = self.visible.get(kw["QueueUrl"], [])
        if not q:
            return {}
        msg = q.pop(0)
        self.inflight[msg["ReceiptHandle"]] = (kw["QueueUrl"], msg)
        return {"Messages": [msg]}

    def change_message_visibility(self, **kw):
        """Make an in-flight message visible again (timing is not simulated); like SQS, the
        redelivery bumps ApproximateReceiveCount and the body is untouched."""
        if self.err:
            raise self.err
        self.visibility_changes.append(kw)
        url, msg = self.inflight.pop(kw["ReceiptHandle"])
        count = int((msg.get("Attributes") or {}).get("ApproximateReceiveCount", "1"))
        redelivered = dict(msg, Attributes={"ApproximateReceiveCount": str(count + 1)})
        self.visible.setdefault(url, []).append(redelivered)
        return {}

    def delete_message(self, **kw):
        if self.err:
            raise self.err
        self.deleted.append(kw["ReceiptHandle"])
        self.inflight.pop(kw["ReceiptHandle"], None)
        return {}

    def seed(self, url: str, body: str, receive_count: int) -> None:
        self._n += 1
        self.visible.setdefault(url, []).append(
            {
                "Body": body,
                "ReceiptHandle": f"seed-{self._n}",
                "Attributes": {"ApproximateReceiveCount": str(receive_count)},
            }
        )


class FailingDeleteSQS(FakeSQS):
    """FakeSQS whose ``DeleteMessage`` fails (e.g. an expired receipt handle / missing IAM grant)."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_attempts = 0

    def delete_message(self, **kw):
        self.delete_attempts += 1
        raise RuntimeError("DeleteMessage denied")


@contextlib.contextmanager
def _no_warnings(case: unittest.TestCase, name: str):
    """``assertNoLogs`` for Python 3.9 (it only exists from 3.10): a sentinel record keeps
    ``assertLogs`` from failing, and must be the only one captured."""
    with case.assertLogs(name, level="WARNING") as cm:
        yield
        logging.getLogger(name).warning("sentinel")
    case.assertEqual(cm.output, [f"WARNING:{name}:sentinel"])


def _attr(sent: dict, key: str) -> str:
    return sent["MessageAttributes"][key]["StringValue"]


class SqsTransportUnitTest(unittest.TestCase):
    def _tr(self, **kw) -> tuple[SqsTransport, FakeSQS]:
        fake = FakeSQS()
        return SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake", **kw), fake

    def test_publish_projects_contract_attributes(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042}, queue="orders")
        body = EnvelopeCodec.encode(env)
        tr.publish("orders", body)

        self.assertEqual(len(fake.sent), 1)
        sent = fake.sent[0]
        self.assertEqual(sent["QueueUrl"], "http://fake/orders")
        self.assertEqual(sent["MessageBody"], body)  # byte-identical
        self.assertEqual(_attr(sent, "bq-job"), env["job"])
        self.assertEqual(_attr(sent, "bq-trace-id"), env["trace_id"])
        self.assertEqual(_attr(sent, "bq-message-id"), env["meta"]["id"])
        self.assertEqual(_attr(sent, "bq-schema-version"), "1")
        self.assertEqual(_attr(sent, "bq-source-lang"), "python")
        self.assertEqual(_attr(sent, "bq-created-at"), str(env["meta"]["created_at"]))
        # Type discipline: ids String, counters Number.
        self.assertEqual(sent["MessageAttributes"]["bq-job"]["DataType"], "String")
        self.assertEqual(sent["MessageAttributes"]["bq-schema-version"]["DataType"], "Number")

    def test_pop_reconciles_attempts_from_receive_count(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        fake.seed("http://fake/orders", EnvelopeCodec.encode(env), 3)  # 3rd delivery -> attempts 2
        msg = tr.pop("orders", timeout=0)
        self.assertIsNotNone(msg)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

    def test_pop_does_not_lower_runtime_attempts(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
        env["attempts"] = 5
        fake.seed("http://fake/default", EnvelopeCodec.encode(env), 1)
        msg = tr.pop("default", timeout=0)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 5)

    def test_pop_empty_returns_none(self):
        tr, _ = self._tr()
        self.assertIsNone(tr.pop("orders", timeout=0))

    def test_ack_deletes_by_receipt_handle(self):
        tr, fake = self._tr()
        fake.seed("http://fake/orders", '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0}', 1)
        msg = tr.pop("orders", timeout=0)
        tr.ack(msg)
        self.assertEqual(fake.deleted, [msg.handle])

    def test_ack_noop_on_empty_handle(self):
        tr, fake = self._tr()
        tr.ack(ReceivedMessage(body="", queue="orders", handle=None))
        self.assertEqual(fake.deleted, [])

    def test_fifo_sets_group_and_dedup(self):
        tr, fake = self._tr(fifo=True)
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders.fifo")
        tr.publish("orders.fifo", EnvelopeCodec.encode(env))
        sent = fake.sent[0]
        self.assertEqual(sent["MessageGroupId"], "orders.fifo")
        self.assertEqual(sent["MessageDeduplicationId"], env["meta"]["id"])

    def test_content_dedup_omits_dedup_id(self):
        tr, fake = self._tr(fifo=True, content_dedup=True, message_group_id="grp")
        tr.publish("orders.fifo", '{"job":"u","trace_id":"t","data":{},"meta":{"id":"m1","schema_version":1},"attempts":0}')
        sent = fake.sent[0]
        self.assertEqual(sent["MessageGroupId"], "grp")
        self.assertNotIn("MessageDeduplicationId", sent)

    def test_resolve_via_get_queue_url_and_caches(self):
        fake = FakeSQS()
        tr = SqsTransport("sqs://", client=fake)  # no prefix -> GetQueueUrl path
        body = '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1,"lang":"python"},"attempts":0}'
        for _ in range(3):
            tr.publish("orders", body)
        self.assertEqual(fake.sent[0]["QueueUrl"], "http://fake/orders")
        self.assertEqual(fake.get_url_calls, 1)  # cached

    def test_pop_applies_visibility_and_wait_options(self):
        tr, fake = self._tr(visibility_timeout=45, wait_time=5)
        tr.pop("orders", timeout=30)
        self.assertEqual(fake.last_receive["VisibilityTimeout"], 45)
        self.assertEqual(fake.last_receive["WaitTimeSeconds"], 5)  # 30 -> clamp 20 -> cap 5
        self.assertEqual(fake.last_receive["MessageAttributeNames"], ["All"])
        self.assertEqual(fake.last_receive["AttributeNames"], ["ApproximateReceiveCount"])

    def test_url_parses_region_endpoint_prefix_fifo(self):
        fake = FakeSQS()
        tr = SqsTransport(
            "sqs://eu-west-1?endpoint=http://ls:4566&prefix=http://ls:4566/000000000000&fifo=1&group_id=g&wait_time=7",
            client=fake,
        )
        self.assertEqual(tr._region, "eu-west-1")
        self.assertEqual(tr._endpoint, "http://ls:4566")
        self.assertTrue(tr._fifo)
        self.assertEqual(tr._message_group_id, "g")
        self.assertEqual(tr._wait_time, 7)
        # prefix resolution avoids GetQueueUrl
        tr.publish("orders.fifo", '{"job":"u","trace_id":"t","data":{},"meta":{"id":"m","schema_version":1}}')
        self.assertEqual(fake.sent[0]["QueueUrl"], "http://ls:4566/000000000000/orders.fifo")

    def test_reconcile_ignores_garbage_and_undecodable(self):
        tr, fake = self._tr()
        fake.seed("http://fake/orders", '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":4}', 0)
        fake.visible["http://fake/orders"][0]["Attributes"]["ApproximateReceiveCount"] = "not-a-number"
        msg = tr.pop("orders", timeout=0)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 4)

        fake.seed("http://fake/orders", "not-json", 3)  # rc>1 but undecodable body
        msg2 = tr.pop("orders", timeout=0)
        self.assertEqual(msg2.body, "not-json")

    def test_attributes_empty_for_undecodable_body(self):
        self.assertEqual(SqsTransport._attributes("}{not json"), {})

    def test_errors_propagate(self):
        boom = RuntimeError("boom")
        fail = SqsTransport("sqs://", client=FakeSQS(err=boom))  # no prefix -> GetQueueUrl errors
        with self.assertRaises(RuntimeError):
            fail.publish("orders", '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}')
        with self.assertRaises(RuntimeError):
            fail.pop("orders", timeout=0)
        with self.assertRaises(RuntimeError):
            fail.ack(ReceivedMessage(body="", queue="orders", handle="h"))

    def test_round_trip_through_app(self):
        fake = FakeSQS()
        tr = SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake")
        app = BabelQueue(transport=tr, queue="orders")
        seen: dict = {}

        @app.handler("urn:babel:orders:created")
        def _on(data, meta):
            seen.update({"data": data, "meta": meta})

        msg_id = app.publish("urn:babel:orders:created", {"order_id": 7})
        processed = app.consume("orders", max_messages=1, timeout=0)
        self.assertEqual(processed, 1)
        self.assertEqual(seen["data"]["order_id"], 7)
        self.assertEqual(seen["meta"]["id"], msg_id)
        self.assertEqual(len(fake.deleted), 1)  # acked/deleted

    # -- R0-C4(a): release via ChangeMessageVisibility (§3.5) ---------------

    def test_transport_is_a_redeliverer(self):
        tr, _ = self._tr()
        self.assertIsInstance(tr, Redeliverer)

    def test_redeliver_changes_visibility_and_keeps_message(self):
        tr, fake = self._tr()
        tr.publish("orders", EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:o:c", {"a": 1})))
        msg = tr.pop("orders", timeout=0)
        assert msg is not None
        tr.redeliver(msg, "ignored-body", 45)

        self.assertEqual(
            fake.visibility_changes,
            [{"QueueUrl": "http://fake/orders", "ReceiptHandle": msg.handle, "VisibilityTimeout": 45}],
        )
        self.assertEqual(fake.deleted, [])  # never deleted
        self.assertEqual(len(fake.sent), 1)  # no copy sent
        again = tr.pop("orders", timeout=0)
        assert again is not None
        self.assertEqual(EnvelopeCodec.decode(again.body)["attempts"], 1)  # broker-counted

    def test_redeliver_clamps_visibility_timeout(self):
        tr, fake = self._tr()
        for delay in (-5, 99999, 2.9):
            fake.seed("http://fake/orders", '{"job":"u","attempts":0}', 1)
            msg = tr.pop("orders", timeout=0)
            assert msg is not None
            tr.redeliver(msg, msg.body, delay)
            tr.pop("orders", timeout=0)  # drain the redelivered copy
        self.assertEqual([c["VisibilityTimeout"] for c in fake.visibility_changes], [0, 43200, 2])

    def test_redeliver_clamps_out_of_range_delays_with_a_warning(self):
        tr, fake = self._tr()
        cases = [(float("inf"), 43200), (float("-inf"), 0), (float("nan"), 0), (-1, 0), (50000, 43200)]
        for delay, expected in cases:
            with self.subTest(delay=delay):
                fake.seed("http://fake/orders", '{"job":"u","attempts":0}', 1)
                msg = tr.pop("orders", timeout=0)
                assert msg is not None
                with self.assertLogs("babelqueue.sqs", level="WARNING"):
                    tr.redeliver(msg, msg.body, delay)  # never raises (no OverflowError)
                self.assertEqual(fake.visibility_changes[-1]["VisibilityTimeout"], expected)
                tr.pop("orders", timeout=0)

    def test_redeliver_in_range_delay_does_not_warn(self):
        tr, fake = self._tr()
        fake.seed("http://fake/orders", '{"job":"u","attempts":0}', 1)
        msg = tr.pop("orders", timeout=0)
        assert msg is not None
        with _no_warnings(self, "babelqueue.sqs"):
            tr.redeliver(msg, msg.body, 3600)
        self.assertEqual(fake.visibility_changes[-1]["VisibilityTimeout"], 3600)

    def test_redeliver_without_receive_count_still_changes_visibility(self):
        tr, fake = self._tr()
        fake.visible["http://fake/orders"] = [{"Body": "orig", "ReceiptHandle": "rh-x"}]
        msg = tr.pop("orders", timeout=0)
        assert msg is not None
        with self.assertLogs("babelqueue.sqs", level="WARNING") as logs:
            tr.redeliver(msg, "advanced-copy", 3600)

        self.assertIn("ApproximateReceiveCount", logs.output[0])
        self.assertEqual(fake.visibility_changes[-1]["VisibilityTimeout"], 3600)
        self.assertEqual(fake.sent, [])  # never re-sent
        self.assertEqual(fake.deleted, [])  # never deleted

    def test_redeliver_on_fifo_changes_visibility_no_copy(self):
        tr, fake = self._tr(fifo=True)
        fake.seed("http://fake/orders.fifo", '{"job":"u","attempts":0}', 1)
        msg = tr.pop("orders.fifo", timeout=0)
        assert msg is not None
        tr.redeliver(msg, "advanced-copy", 30)
        self.assertEqual(fake.visibility_changes[-1]["VisibilityTimeout"], 30)
        self.assertEqual(fake.sent, [])  # no dedup-id copy that SQS could swallow
        self.assertEqual(fake.deleted, [])

    def test_redeliver_without_receipt_handle_sends_nothing(self):
        tr, fake = self._tr()
        msg = ReceivedMessage(body="b", queue="orders", handle=None)
        with self.assertLogs("babelqueue.sqs", level="WARNING"):
            tr.redeliver(msg, "advanced-copy", 0)
        self.assertEqual((fake.sent, fake.deleted, fake.visibility_changes), ([], [], []))

    def test_projection_does_not_log_forbidden_key_drop(self):
        """The body is sent unchanged, so the attribute/dedup projection must not claim to
        have dropped a forbidden key."""
        tr, fake = self._tr(fifo=True)
        env = EnvelopeCodec.make("urn:babel:o:c", {})
        raw = EnvelopeCodec.encode(env)[:-1] + ',"timestamp":1}'
        with _no_warnings(self, "babelqueue.codec"):
            tr.publish("orders.fifo", raw)
        self.assertEqual(fake.sent[-1]["MessageBody"], raw)
        self.assertEqual(fake.sent[-1]["MessageDeduplicationId"], env["meta"]["id"])

    def test_app_retry_defaults_to_immediate_visibility_release(self):
        fake = FakeSQS()
        tr = SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake")
        app = BabelQueue(transport=tr, queue="orders", max_attempts=2)

        @app.handler("urn:babel:orders:created")
        def _on(data, meta):
            raise RuntimeError("always")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        app.consume("orders", max_messages=2, timeout=0)

        self.assertEqual([c["VisibilityTimeout"] for c in fake.visibility_changes], [0])
        self.assertEqual(len(fake.sent), 1)  # never re-sent; 2nd failure exhausts -> ack

    def test_app_retry_releases_with_backoff_not_republish(self):
        fake = FakeSQS()
        tr = SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake")
        app = BabelQueue(transport=tr, queue="orders", max_attempts=3, retry_backoff=30)
        calls: list[int] = []

        @app.handler("urn:babel:orders:created")
        def _on(data, meta, envelope):
            calls.append(envelope["attempts"])
            if len(calls) < 2:
                raise RuntimeError("transient")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        app.consume("orders", max_messages=2, timeout=0)

        self.assertEqual(calls, [0, 1])  # second delivery counted by the broker
        self.assertEqual(len(fake.sent), 1)  # the retry did not publish a copy
        self.assertEqual(len(fake.visibility_changes), 1)
        self.assertEqual(fake.visibility_changes[0]["VisibilityTimeout"], 30)
        self.assertEqual(fake.deleted, [fake.visibility_changes[0]["ReceiptHandle"]])

    def test_app_unknown_urn_release_uses_visibility(self):
        fake = FakeSQS()
        tr = SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake")
        app = BabelQueue(
            transport=tr,
            queue="orders",
            on_unknown_urn=UnknownUrnStrategy.RELEASE,
            unknown_urn_release_delay=120,
        )
        app.publish("urn:babel:nobody:listens", {})
        app.consume("orders", max_messages=1, timeout=0)

        self.assertEqual(len(fake.sent), 1)  # no re-publish
        self.assertEqual(fake.deleted, [])
        self.assertEqual(fake.visibility_changes[0]["VisibilityTimeout"], 120)
        self.assertEqual(len(fake.visible["http://fake/orders"]), 1)  # still on the queue


    # -- A failed DeleteMessage after success is not a handler failure ------

    def _failing_delete_app(self, **kw):
        fake = FailingDeleteSQS()
        tr = SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake")
        return BabelQueue(transport=tr, queue="orders", **kw), fake

    def test_app_failed_delete_after_success_is_not_released(self):
        app, fake = self._failing_delete_app(max_attempts=3)
        calls: list[int] = []

        @app.handler("urn:babel:orders:created")
        def _on(data, meta):
            calls.append(1)

        app.publish("urn:babel:orders:created", {"order_id": 1})
        with self.assertLogs("babelqueue.app", level="ERROR") as cm:
            processed = app.consume("orders", max_messages=1, timeout=0)

        self.assertEqual(processed, 1)  # the loop survives
        self.assertEqual(calls, [1])  # handled once, not retried
        self.assertEqual(fake.delete_attempts, 1)
        self.assertEqual(fake.visibility_changes, [])  # NOT released (no immediate redelivery)
        self.assertEqual(len(fake.sent), 1)  # NOT re-published / dead-lettered
        self.assertEqual(len(cm.records), 1)
        self.assertIn("Failed to acknowledge a processed message", cm.output[0])
        self.assertIsNotNone(cm.records[0].exc_info)  # the broker error is attached

    def test_app_failed_delete_on_unknown_urn_delete_is_not_released(self):
        app, fake = self._failing_delete_app(on_unknown_urn=UnknownUrnStrategy.DELETE)
        app.publish("urn:babel:nobody:listens", {})
        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume("orders", max_messages=1, timeout=0)
        self.assertEqual(fake.delete_attempts, 1)
        self.assertEqual(fake.visibility_changes, [])
        self.assertEqual(len(fake.sent), 1)

    def test_app_failed_delete_after_dead_letter_is_not_released(self):
        app, fake = self._failing_delete_app(max_attempts=1, dead_letter=True)

        @app.handler("urn:babel:orders:created")
        def _on(data, meta):
            raise RuntimeError("always")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume("orders", max_messages=1, timeout=0)
        dlq = [m for m in fake.sent if m["QueueUrl"].endswith("orders.dlq")]
        self.assertEqual(len(dlq), 1)  # dead-lettered exactly once
        self.assertEqual(fake.visibility_changes, [])  # and not released on top of that

    # -- ADR-0028: traceparent on MessageAttributes ------------------------

    def test_transport_is_a_header_publisher(self):
        tr, _ = self._tr()
        self.assertIsInstance(tr, HeaderPublisher)

    def test_publish_with_headers_projects_traceparent_attribute(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        body = EnvelopeCodec.encode(env)
        tr.publish_with_headers("orders", body, {"traceparent": "00-abc"})
        sent = fake.sent[0]
        self.assertEqual(sent["MessageBody"], body)  # body unchanged
        self.assertEqual(_attr(sent, "traceparent"), "00-abc")
        self.assertEqual(sent["MessageAttributes"]["traceparent"]["DataType"], "String")
        # contract attributes still present beside the header
        self.assertEqual(_attr(sent, "bq-job"), env["job"])

    def test_contract_attribute_wins_a_collision(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        # a header that collides with a contract key must not clobber it
        tr.publish_with_headers("orders", EnvelopeCodec.encode(env), {"bq-job": "evil"})
        self.assertEqual(_attr(fake.sent[0], "bq-job"), env["job"])

    def test_header_merge_respects_ten_attribute_cap(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        # 6 contract attrs are projected; only 4 more headers fit under the SQS cap of 10
        headers = {f"h{i}": str(i) for i in range(8)}
        tr.publish_with_headers("orders", EnvelopeCodec.encode(env), headers)
        self.assertEqual(len(fake.sent[0]["MessageAttributes"]), 10)

    def test_pop_surfaces_message_attributes_as_headers(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish_with_headers("orders", EnvelopeCodec.encode(env), {"traceparent": "00-xyz"})
        msg = tr.pop("orders", timeout=0)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.headers.get("traceparent"), "00-xyz")

    def test_plain_publish_then_pop_has_no_extra_headers(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))
        msg = tr.pop("orders", timeout=0)
        # only the contract bq-* attributes surface; there is no traceparent
        self.assertNotIn("traceparent", msg.headers)

    def test_make_transport_routes_sqs_scheme(self):
        # The scheme dispatches to SqsTransport; without boto3 it surfaces a clear
        # install hint (covers the make_transport branch). With boto3 present this
        # would construct a real client, so only assert the branch is reached.
        try:
            import boto3  # noqa: F401
            self.skipTest("boto3 installed — real client would be built")
        except ImportError:
            with self.assertRaises(ImportError):
                make_transport("sqs://")


def _sqs_available() -> bool:
    try:
        import boto3
    except ImportError:
        return False
    endpoint = os.environ.get("SQS_ENDPOINT", "http://localhost:4566")
    try:
        client = boto3.client(
            "sqs",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        client.list_queues()
        return True
    except Exception:  # pragma: no cover - connection failure
        return False


@unittest.skipUnless(_sqs_available(), "no reachable LocalStack SQS at SQS_ENDPOINT")
class SqsLocalStackIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        import boto3

        self.endpoint = os.environ.get("SQS_ENDPOINT", "http://localhost:4566")
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        self.raw = boto3.client("sqs", region_name=self.region, endpoint_url=self.endpoint)
        self.queue = f"bq-it-{uuid.uuid4().hex}"
        self.raw.create_queue(QueueName=self.queue)

    def tearDown(self) -> None:
        try:
            url = self.raw.get_queue_url(QueueName=self.queue)["QueueUrl"]
            self.raw.delete_queue(QueueUrl=url)
        except Exception:
            pass

    def test_produce_consume_round_trip(self):
        url = f"sqs://{self.region}?endpoint={self.endpoint}"
        app = BabelQueue(url, queue=self.queue)
        seen: dict = {}
        app.register("urn:babel:orders:created", lambda data, meta: seen.update(data))
        msg_id = app.publish("urn:babel:orders:created", {"order_id": 1042})
        self.assertTrue(msg_id)

        # publish carried the contract attributes; verify them off the raw queue too
        processed = 0
        for _ in range(30):
            processed = app.consume(self.queue, max_messages=1, timeout=1)
            if processed:
                break
        self.assertEqual(processed, 1)
        self.assertEqual(seen.get("order_id"), 1042)

    def test_traceparent_round_trips_on_message_attributes(self):
        """ADR-0028: a published traceparent arrives on the consumed message's headers via SQS
        MessageAttributes, body unchanged."""
        url = f"sqs://{self.region}?endpoint={self.endpoint}"
        tr = SqsTransport(url)
        body = EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1}, queue=self.queue)
        )
        tr.publish_with_headers(self.queue, body, {"traceparent": "00-localstack"})
        msg = None
        for _ in range(30):
            msg = tr.pop(self.queue, timeout=1)
            if msg is not None:
                break
        self.assertIsNotNone(msg)
        self.assertEqual(msg.body, body)
        self.assertEqual(msg.headers.get("traceparent"), "00-localstack")
        tr.ack(msg)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
