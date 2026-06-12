"""Amazon SQS transport. Requires the ``sqs`` extra:

    pip install "babelqueue[sqs]"

Producing sends the canonical envelope as the message body and projects the
contract envelope fields onto native SQS ``MessageAttributes`` (``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-created-at``) — so a Go/PHP/... peer can route on ``bq-job``
and correlate on ``bq-trace-id`` without parsing the body. Consuming uses the
visibility-timeout reservation model (``receive_message`` -> process ->
``delete_message``); the authoritative attempt count is the broker's
``ApproximateReceiveCount``, reconciled onto the envelope as ``attempts = count - 1``.

This implements §3 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); SQS is purely additive.

URL form: ``sqs://[region][?endpoint=...&prefix=...&fifo=1&group_id=...&wait_time=20]``
(e.g. ``sqs://us-east-1?endpoint=http://localhost:4566`` for LocalStack). Credentials
come from the standard AWS default provider chain. For richer setups, build the
transport directly and pass it via ``BabelQueue(transport=...)``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlsplit

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport


class SqsTransport(Transport):
    def __init__(
        self,
        url: str = "sqs://",
        *,
        client: Any = None,
        region: Optional[str] = None,
        endpoint: Optional[str] = None,
        queue_url_prefix: Optional[str] = None,
        wait_time: Optional[int] = None,
        visibility_timeout: Optional[int] = None,
        fifo: bool = False,
        message_group_id: Optional[str] = None,
        content_dedup: bool = False,
    ) -> None:
        parts = urlsplit(url) if url else urlsplit("sqs://")
        q = parse_qs(parts.query)

        self._region = region or (parts.hostname or None)
        self._endpoint = endpoint or _q1(q, "endpoint")
        self._queue_url_prefix = queue_url_prefix or _q1(q, "prefix")
        self._wait_time = wait_time if wait_time is not None else _qint(q, "wait_time")
        self._visibility_timeout = (
            visibility_timeout if visibility_timeout is not None else _qint(q, "visibility_timeout")
        )
        self._fifo = fifo or _qbool(q, "fifo")
        self._message_group_id = message_group_id or _q1(q, "group_id")
        self._content_dedup = content_dedup or _qbool(q, "content_dedup")
        self._urls: Dict[str, str] = {}

        if client is not None:
            self._sqs = client
            return
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "SqsTransport requires the 'boto3' package. Install with "
                'pip install "babelqueue[sqs]".'
            ) from exc
        kwargs: Dict[str, Any] = {}
        if self._region:
            kwargs["region_name"] = self._region
        if self._endpoint:
            kwargs["endpoint_url"] = self._endpoint
        self._sqs = boto3.client("sqs", **kwargs)  # pragma: no cover - needs AWS/LocalStack

    # -- helpers ------------------------------------------------------------

    def _resolve_url(self, name: str) -> str:
        cached = self._urls.get(name)
        if cached is not None:
            return cached
        if self._queue_url_prefix:
            url = self._queue_url_prefix.rstrip("/") + "/" + name
        else:
            url = self._sqs.get_queue_url(QueueName=name)["QueueUrl"]
        self._urls[name] = url
        return url

    @staticmethod
    def _attributes(body: str) -> Dict[str, Dict[str, str]]:
        """Project the envelope's contract fields onto SQS MessageAttributes — a
        redundant, routable view of the body (the body stays authoritative)."""
        try:
            env: Dict[str, Any] = EnvelopeCodec.decode(body)
        except (ValueError, TypeError):  # pragma: no cover - decode is defensive
            return {}
        meta = env.get("meta") or {}

        def s(v: Any) -> Dict[str, str]:
            return {"DataType": "String", "StringValue": str(v)}

        def n(v: Any) -> Dict[str, str]:
            return {"DataType": "Number", "StringValue": str(v)}

        attrs: Dict[str, Dict[str, str]] = {}
        if env.get("job"):
            attrs["bq-job"] = s(env["job"])
        if env.get("trace_id"):
            attrs["bq-trace-id"] = s(env["trace_id"])
        if meta.get("id"):
            attrs["bq-message-id"] = s(meta["id"])
        if meta.get("schema_version") is not None:
            attrs["bq-schema-version"] = n(meta["schema_version"])
        if meta.get("lang"):
            attrs["bq-source-lang"] = s(meta["lang"])
        if meta.get("created_at") is not None:
            attrs["bq-created-at"] = n(meta["created_at"])
        return attrs

    @staticmethod
    def _reconcile(body: str, receive_count: Any) -> str:
        """Set attempts to max(current, ApproximateReceiveCount - 1): a first delivery
        reads 0, a natively-redelivered message reflects its true count, and a
        runtime-incremented counter is never lowered."""
        try:
            rc = int(receive_count)
        except (ValueError, TypeError):
            return body
        if rc <= 1:
            return body
        env = EnvelopeCodec.decode(body)
        if not env:
            return body
        native = rc - 1
        if native <= int(env.get("attempts", 0)):
            return body
        env["attempts"] = native
        return EnvelopeCodec.encode(env)

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        params: Dict[str, Any] = {"QueueUrl": self._resolve_url(queue), "MessageBody": body}
        attrs = self._attributes(body)
        if attrs:
            params["MessageAttributes"] = attrs
        if self._fifo:
            params["MessageGroupId"] = self._message_group_id or queue
            if not self._content_dedup:
                msg_id = (EnvelopeCodec.decode(body).get("meta") or {}).get("id")
                if msg_id:
                    params["MessageDeduplicationId"] = msg_id
        self._sqs.send_message(**params)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = int(timeout) if timeout and timeout > 0 else 0
        if wait > 20:
            wait = 20
        if self._wait_time is not None and self._wait_time < wait:
            wait = self._wait_time
        params: Dict[str, Any] = {
            "QueueUrl": self._resolve_url(queue),
            "MaxNumberOfMessages": 1,
            "WaitTimeSeconds": wait,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["ApproximateReceiveCount"],
        }
        if self._visibility_timeout is not None:
            params["VisibilityTimeout"] = self._visibility_timeout
        resp = self._sqs.receive_message(**params)
        messages = resp.get("Messages") or []
        if not messages:
            return None
        msg = messages[0]
        body = msg.get("Body", "")
        receive_count = (msg.get("Attributes") or {}).get("ApproximateReceiveCount")
        if receive_count is not None:
            body = self._reconcile(body, receive_count)
        return ReceivedMessage(body=body, queue=queue, handle=msg.get("ReceiptHandle"))

    def ack(self, message: ReceivedMessage) -> None:
        if not message.handle:
            return
        self._sqs.delete_message(
            QueueUrl=self._resolve_url(message.queue), ReceiptHandle=message.handle
        )


def _q1(q: Dict[str, list], key: str) -> Optional[str]:
    values = q.get(key)
    return values[0] if values else None


def _qint(q: Dict[str, list], key: str) -> Optional[int]:
    v = _q1(q, key)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:  # pragma: no cover - defensive
        return None


def _qbool(q: Dict[str, list], key: str) -> bool:
    v = _q1(q, key)
    return v is not None and v.lower() in ("1", "true", "yes", "on")
