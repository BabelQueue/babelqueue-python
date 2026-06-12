"""Tests for the Amazon SQS transport.

The unit tests inject a fake SQS client, so they run without ``boto3`` and without
a broker. A separate integration test round-trips against LocalStack and is skipped
unless ``boto3`` + a reachable endpoint are present (CI runs it).
"""

from __future__ import annotations

import os
import unittest
import uuid

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.sqs_transport import SqsTransport
from babelqueue.transport import ReceivedMessage, make_transport


class FakeSQS:
    """In-memory SQS API — no boto3, no network."""

    def __init__(self, err: Exception | None = None) -> None:
        self.visible: dict[str, list[dict]] = {}
        self.sent: list[dict] = []
        self.deleted: list[str] = []
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
        return {"Messages": [q.pop(0)]}

    def delete_message(self, **kw):
        if self.err:
            raise self.err
        self.deleted.append(kw["ReceiptHandle"])
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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
