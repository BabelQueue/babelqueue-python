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
Releasing a message for retry (handler failure, unknown-URN ``release``, graceful shutdown)
always uses ``ChangeMessageVisibility(VisibilityTimeout=backoff)`` on the received message — it
is never deleted and never re-sent, and the broker keeps counting deliveries (§3.5). The default
backoff is 0 s (immediate redelivery). Because every release bumps ``ApproximateReceiveCount``, a
message that always fails loops until the SDK's ``max_attempts`` dead-letters it; configure a
native ``RedrivePolicy`` (``maxReceiveCount``) on the queue as the broker-side backstop against
poison-message loops. A shutdown release counts as a delivery too.

Out-of-band transport headers (e.g. a W3C ``traceparent`` for cross-hop span linkage, ADR-0028)
ride the native SQS ``MessageAttributes`` (String) beside the contract ``bq-*`` attributes (the
contract keys win a collision, and the merge is bounded by SQS's 10-attribute limit);
:meth:`~SqsTransport.pop` already requests ``MessageAttributeNames: ["All"]`` and maps the
non-``bq-`` attributes back onto :attr:`~babelqueue.transport.ReceivedMessage.headers`. The frozen
envelope is untouched (GR-1).

This implements §3 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); SQS is purely additive.

URL form: ``sqs://[region][?endpoint=...&prefix=...&fifo=1&group_id=...&wait_time=20]``
(e.g. ``sqs://us-east-1?endpoint=http://localhost:4566`` for LocalStack). Credentials
come from the standard AWS default provider chain. For richer setups, build the
transport directly and pass it via ``BabelQueue(transport=...)``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Set
from urllib.parse import parse_qs, urlsplit

from .codec import EnvelopeCodec, parse_envelope
from .transport import ReceivedMessage, Transport

#: SQS's upper bound for ``ChangeMessageVisibility`` (12 hours).
MAX_VISIBILITY_TIMEOUT = 43200

logger = logging.getLogger("babelqueue.sqs")


def _visibility_seconds(delay: float) -> int:
    """Clamp a backoff in seconds onto SQS's 0..43200 integer visibility range.

    A value above 43200 (including ``inf``) is clamped to 43200, a negative one (including
    ``-inf``) to 0, and a non-numeric one or ``nan`` to 0 — each with a ``WARNING``, so an
    out-of-range release delay is never silently altered. Fractions are truncated."""
    try:
        seconds = int(delay)
    except OverflowError:  # +/-inf
        seconds = MAX_VISIBILITY_TIMEOUT if delay > 0 else 0
    except (ValueError, TypeError):  # nan, garbage
        logger.warning("Invalid SQS release delay %r; using 0 s.", delay)
        return 0
    clamped = max(0, min(seconds, MAX_VISIBILITY_TIMEOUT))
    if delay < 0 or delay > MAX_VISIBILITY_TIMEOUT:
        logger.warning(
            "SQS release delay %r s is outside 0..%d; clamped to %d s.",
            delay, MAX_VISIBILITY_TIMEOUT, clamped,
        )
    return clamped


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
        # Receipt handles whose delivery did NOT report ApproximateReceiveCount: their release
        # still uses ChangeMessageVisibility, but attempts cannot advance — warned on release.
        self._uncounted_handles: Set[str] = set()

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
        # A read-only projection: parse without the forbidden-key drop/warning (the body is
        # sent unchanged, so logging a "dropped" key here would be false).
        env: Dict[str, Any] = parse_envelope(body)
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
    def _merge_attributes(
        contract: Dict[str, Dict[str, str]], headers: Optional[Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        """Merge out-of-band ``headers`` (as String attributes) onto the contract attributes.

        Contract ``bq-*`` attributes win a key collision; blank keys/values are dropped. SQS allows
        at most 10 message attributes, so once the table is full further headers are skipped (the
        contract attributes are always kept). Returns a fresh dict.
        """
        merged: Dict[str, Dict[str, str]] = dict(contract)
        for key, value in (headers or {}).items():
            if not key or value is None or value == "":
                continue
            if key in merged:  # contract attribute wins
                continue
            if len(merged) >= 10:  # SQS hard cap on MessageAttributes
                break
            merged[str(key)] = {"DataType": "String", "StringValue": str(value)}
        return merged

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
        env = parse_envelope(body)
        if not env:
            return body
        native = rc - 1
        if native <= int(env.get("attempts", 0)):
            return body
        env["attempts"] = native
        return EnvelopeCodec.encode(env)

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        self._send(queue, body, None)

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        """Send ``body`` with out-of-band ``headers`` projected onto SQS ``MessageAttributes``
        beside the contract ``bq-*`` attributes (:class:`HeaderPublisher`, ADR-0028). The contract
        attributes win a key collision and the merge is capped at SQS's 10-attribute limit."""
        self._send(queue, body, headers)

    def _send(self, queue: str, body: str, headers: Optional[Dict[str, str]]) -> None:
        params: Dict[str, Any] = {"QueueUrl": self._resolve_url(queue), "MessageBody": body}
        attrs = self._merge_attributes(self._attributes(body), headers)
        if attrs:
            params["MessageAttributes"] = attrs
        if self._fifo:
            params["MessageGroupId"] = self._message_group_id or queue
            if not self._content_dedup:
                msg_id = (parse_envelope(body).get("meta") or {}).get("id")
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
        headers = _message_attribute_headers(msg.get("MessageAttributes"))
        handle = msg.get("ReceiptHandle")
        if handle and receive_count is None:
            self._uncounted_handles.add(handle)
        return ReceivedMessage(body=body, queue=queue, handle=handle, headers=headers)

    def ack(self, message: ReceivedMessage) -> None:
        if not message.handle:
            return
        self._uncounted_handles.discard(message.handle)
        self._sqs.delete_message(
            QueueUrl=self._resolve_url(message.queue), ReceiptHandle=message.handle
        )

    def redeliver(self, message: ReceivedMessage, body: str, delay: float) -> None:
        """Release ``message`` for redelivery after ``delay`` seconds (broker-bindings.md §3.5;
        :class:`~babelqueue.transport.Redeliverer`).

        Always ``ChangeMessageVisibility(VisibilityTimeout=delay)`` on the received message —
        clamped to SQS's 0..43200 s with a ``WARNING`` when the delay had to change — and the
        message is NOT deleted and ``body`` is NOT sent: SQS's ``ApproximateReceiveCount`` is the
        attempt counter. Every release therefore counts as a delivery, including a graceful
        shutdown release; pair the SDK's ``max_attempts`` with a queue ``RedrivePolicy``
        (``maxReceiveCount``) to bound poison-message loops.

        If the delivery did not report ``ApproximateReceiveCount`` the release still happens, but
        ``attempts`` cannot advance (a ``WARNING`` says so). A message without a receipt handle
        cannot be released at all: nothing is sent (a copy would duplicate it, since the original
        cannot be deleted either) and it reappears once the queue's visibility timeout expires.
        """
        handle = message.handle
        if not handle:
            logger.warning(
                "SQS message on %r has no receipt handle; it cannot be released and will "
                "reappear after the queue's visibility timeout.",
                message.queue,
            )
            return
        if handle in self._uncounted_handles:
            self._uncounted_handles.discard(handle)
            logger.warning(
                "SQS delivery on %r did not report ApproximateReceiveCount; the release cannot "
                "advance attempts — rely on the queue's RedrivePolicy to stop a poison loop.",
                message.queue,
            )
        self._sqs.change_message_visibility(
            QueueUrl=self._resolve_url(message.queue),
            ReceiptHandle=handle,
            VisibilityTimeout=_visibility_seconds(delay),
        )


def _message_attribute_headers(attributes: Any) -> Dict[str, str]:
    """Map an SQS message's ``MessageAttributes`` onto ``Dict[str, str]`` (String values), so
    out-of-band metadata (e.g. a ``traceparent``) surfaces on the received message. Mirrors the Go
    SQS transport, which maps all returned attributes onto ``ReceivedMessage.Headers``. Returns
    ``{}`` when the message carried no attributes."""
    if not isinstance(attributes, dict):
        return {}
    out: Dict[str, str] = {}
    for key, spec in attributes.items():
        if not isinstance(spec, dict):
            continue
        value = spec.get("StringValue")
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


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
