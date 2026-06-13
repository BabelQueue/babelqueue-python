"""Azure Service Bus transport tests.

The native projection and the ``attempts = DeliveryCount - 1`` reconciliation run with no
broker and no ``azure-servicebus`` (the transport's azure import is lazy). The publish flow,
which builds a real ``ServiceBusMessage``, is skipped unless ``azure-servicebus`` is installed.
"""

from __future__ import annotations

import unittest

from babelqueue import EnvelopeCodec
from babelqueue.asb_transport import AsbTransport
from babelqueue.transport import ReceivedMessage, make_transport

try:
    import azure.servicebus  # noqa: F401

    HAVE_ASB = True
except ImportError:
    HAVE_ASB = False

URN = "urn:babel:orders:created"


def body(attempts: int = 0) -> str:
    env = EnvelopeCodec.decode(
        EnvelopeCodec.encode(EnvelopeCodec.make(URN, {"order_id": 1042}, queue="orders"))
    )
    env["attempts"] = attempts
    return EnvelopeCodec.encode(env)


class FakeReceived:
    """Duck-typed ServiceBusReceivedMessage (the real one has no public constructor)."""

    def __init__(self, raw: str, delivery_count: int = 1) -> None:
        self._raw = raw
        self.delivery_count = delivery_count

    def __str__(self) -> str:
        return self._raw


class FakeSender:
    def __init__(self) -> None:
        self.sent: list = []

    def send_messages(self, message) -> None:
        self.sent.append(message)


class FakeReceiver:
    def __init__(self, messages=()) -> None:
        self._messages = list(messages)
        self.completed: list = []

    def receive_messages(self, max_message_count: int = 1, max_wait_time=None):
        batch = self._messages[:max_message_count]
        del self._messages[:max_message_count]
        return batch

    def complete_message(self, message) -> None:
        self.completed.append(message)


class FakeClient:
    def __init__(self, sender=None, receiver=None) -> None:
        self.sender = sender if sender is not None else FakeSender()
        self.receiver = receiver if receiver is not None else FakeReceiver()

    def get_queue_sender(self, queue: str):
        return self.sender

    def get_queue_receiver(self, queue: str):
        return self.receiver


class AsbProjectionTest(unittest.TestCase):
    def test_projection_maps_native_fields_and_properties(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        proj = AsbTransport._projection(raw)

        self.assertEqual(proj["subject"], URN)
        self.assertEqual(proj["correlation_id"], env["trace_id"])
        self.assertEqual(proj["message_id"], env["meta"]["id"])
        self.assertEqual(proj["content_type"], "application/json")
        props = proj["application_properties"]
        self.assertEqual(props["bq-schema-version"], env["meta"]["schema_version"])
        self.assertEqual(props["bq-source-lang"], env["meta"]["lang"])
        self.assertEqual(props["bq-created-at"], env["meta"]["created_at"])

    def test_reconcile_attempts_is_delivery_count_minus_one(self):
        out = AsbTransport._reconcile(body(0), 3)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 2)

    def test_reconcile_first_delivery_is_zero(self):
        out = AsbTransport._reconcile(body(0), 1)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 0)

    def test_reconcile_ignores_garbage_count(self):
        raw = body(5)
        self.assertEqual(AsbTransport._reconcile(raw, "not-a-number"), raw)

    def test_reconcile_never_lowers_runtime_count(self):
        # The runtime retries by republishing with attempts+1 (DeliveryCount back to 1),
        # so a higher body count must win over a lower native one.
        raw = body(5)
        self.assertEqual(int(EnvelopeCodec.decode(AsbTransport._reconcile(raw, 2))["attempts"]), 5)


class AsbConsumeTest(unittest.TestCase):
    def test_pop_reconciles_attempts_and_returns_handle(self):
        received = FakeReceived(body(0), delivery_count=3)
        tr = AsbTransport(client=FakeClient(receiver=FakeReceiver([received])))

        msg = tr.pop("orders")

        self.assertIsNotNone(msg)
        self.assertEqual(msg.queue, "orders")
        self.assertIs(msg.handle, received)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

    def test_pop_empty_returns_none(self):
        tr = AsbTransport(client=FakeClient(receiver=FakeReceiver([])))
        self.assertIsNone(tr.pop("orders"))

    def test_ack_completes_the_reserved_message(self):
        received = FakeReceived(body(), delivery_count=1)
        receiver = FakeReceiver([received])
        tr = AsbTransport(client=FakeClient(receiver=receiver))

        tr.ack(tr.pop("orders"))

        self.assertEqual(receiver.completed, [received])

    def test_ack_noop_without_handle(self):
        tr = AsbTransport(client=FakeClient())
        tr.ack(ReceivedMessage(body="", queue="orders", handle=None))  # no error

    def test_make_transport_routes_sb_scheme(self):
        if HAVE_ASB:
            self.skipTest("azure installed — building a real client would need credentials")
        with self.assertRaises(ImportError):
            make_transport("sb://ns.servicebus.windows.net")


@unittest.skipUnless(HAVE_ASB, "azure-servicebus not installed")
class AsbPublishTest(unittest.TestCase):
    def test_publish_builds_message_with_native_projection(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        sender = FakeSender()
        tr = AsbTransport(client=FakeClient(sender=sender))

        tr.publish("orders", raw)

        self.assertEqual(len(sender.sent), 1)
        sent = sender.sent[0]
        self.assertEqual(sent.subject, URN)
        self.assertEqual(sent.correlation_id, env["trace_id"])
        self.assertEqual(sent.message_id, env["meta"]["id"])
        self.assertEqual(sent.content_type, "application/json")
        self.assertEqual(EnvelopeCodec.urn(EnvelopeCodec.decode(str(sent))), URN)


if __name__ == "__main__":
    unittest.main()
