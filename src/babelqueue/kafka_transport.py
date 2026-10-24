"""Apache Kafka transport. Requires the ``kafka`` extra:

    pip install "babelqueue[kafka]"

Producing writes the canonical envelope as the record value and projects the contract
envelope fields onto native Kafka record headers (UTF-8 byte strings): ``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` — so a Java/.NET/... peer can route on ``bq-job``
without parsing the body — with the record timestamp mirroring ``meta.created_at``.
Consuming is process-then-commit: ``poll`` reserves a record (``enable.auto.commit=false``)
and ``ack`` commits the offset only after the handler returns (at-least-once). A record that is
neither acked nor rewound (its settlement raised) is re-sought by the next ``pop``, so a
restarted consume loop redelivers it instead of committing past it. Kafka has no
native delivery count, so the ``bq-attempts`` header is the authoritative retry counter (the
body's ``attempts`` is the fallback for non-BabelQueue producers); the runtime owns retry by
republishing with attempts+1 and dead-letters to ``<queue>.dlq``.

Settlement writes (retry republish, DLQ) go through ``publish_settle`` and wait for the broker's
delivery report (``delivery_timeout``); a failure raises ``KafkaDeliveryError`` and the offset is
not committed. ``publish`` itself stays fire-and-forget. Those writes also carry the consumed
record's key unchanged (``ReceivedMessage.key``; null stays null, empty stays empty, never
synthesised). A poison record (§6.5: a value that is not valid UTF-8, not valid JSON, not a JSON
object other than ``null``, or a member of the wrong type, member names matched exactly as spelled: what the Go
reference's typed ``Decode`` rejects, see :mod:`babelqueue._envelope_shape`) is handed to the runtime under a
reserved route. With the DLQ on (``dead_letter`` or ``on_unknown_urn=DEAD_LETTER``) it is dead-lettered
directly, never retried, and written in the ADR-0035 shape: a body that is one strict JSON object is
spliced byte for byte, anything else is carried as ``{"raw": "<body>"}`` (base64 plus
``raw_encoding: "base64"`` when it is not valid UTF-8), and a ``dead_letter`` block with
``reason: "poison"`` is added (the key is preserved; the DLQ is the configured one or ``<queue>.dlq``, which must exist). With
the DLQ off it is NOT written to a DLQ (Go ``routePoison``): ``on_unknown_urn=DELETE`` drops it, any other
strategy takes the bounded FAIL path (retried through the tiers up to ``max_attempts``, then dropped).
The record is handed over raw under a
``PoisonRoute`` and wrapped by ``publish_settle`` only when it is written to a queue other than the one
it was read from, so a ``redrive`` restore into the DLQ it popped it from (any DLQ name) is byte for
byte and never double-wraps. The version gate (§6.7) still
dead-letters its record raw. The runtime logs a poison record as a version-gated message because it
reuses that settle path; the route in the log names it.

Delayed delivery and retry back-off are SDK-owned retry topics (§6.4/§6.5, ADR-0034): with
``retry_tiers_ms`` configured, ``publish_delayed`` parks a message on the smallest tier
``<topic>.retry.<n>`` that covers the requested delay, and a retry / release goes to the tier picked
by the record's attempt count (``n = min(attempts, N-1) + 1``; the runtime's delay argument is
ignored there). A :class:`~babelqueue.kafka_retry.RetryReinjector` (the re-inject consumer) moves
each parked record back to its work topic once the tier delay has elapsed. With no tiers
configured both operations raise :class:`BabelQueueError` and nothing is produced.

This implements §6 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Apache Kafka is purely additive.

URL form: ``kafka://host:9092[,host2:9092]``. The default consumer group is ``babelqueue``.
For a custom client, build the transport directly and pass it via ``BabelQueue(transport=...)``
or ``KafkaTransport(producer=..., consumer_factory=...)``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

from . import dead_letter
from .codec import ASCII_WHITESPACE, SCHEMA_VERSION, EnvelopeCodec, parse_envelope
from ._envelope_shape import envelope_violation
from ._poison_record import build_poison_record
from .exceptions import BabelQueueError
from .transport import (
    PoisonRoute,
    ReceivedMessage,
    Transport,
    truncate_declared_version,
)

_logger = logging.getLogger("babelqueue.kafka")

#: A blank ``bq-schema-version``: empty or only ASCII whitespace (space, \t, \n, \v, \f, \r) — the
#: same set in every SDK (GR-5). Deliberately not ``str.strip()``, which also strips U+001C-U+001F,
#: U+0085, U+00A0, U+2028, U+3000 ...
_BLANK_ASCII_PATTERN = re.compile("[" + re.escape(ASCII_WHITESPACE) + "]*")
#: Every surrogate code point except U+DC80..U+DCFF (those are ``surrogateescape``d non-UTF-8 bytes).
_LONE_SURROGATE = re.compile("[\ud800-\udbff\udc00-\udc7f\udd00-\udfff]")
#: Prefix of the reserved ``route_urn`` a version-gated message is delivered under. It is not a
#: ``urn:babel:`` URN, so no handler can legitimately own it; the runtime dead-letters it
#: directly (never retried, never decoded; the DLQ record is the incoming raw body). The
#: rejected ``bq-schema-version`` is named in the logged error.
UNSUPPORTED_SCHEMA_ROUTE_PREFIX = "babelqueue:unsupported-schema-version:"

#: Reserved ``route_urn`` of a record whose value is not valid UTF-8. Like a version-gated record it
#: can never gain a handler; it takes the poison path of :data:`POISON_ROUTE` (``PoisonRoute``). The body carries the raw bytes as ``surrogateescape`` text, which
#: :meth:`KafkaTransport.publish` encodes back to the exact original bytes.
NON_UTF8_ROUTE = UNSUPPORTED_SCHEMA_ROUTE_PREFIX + "non-utf8-payload"

#: Reserved ``route_urn`` of a record whose value is not a decodable BabelQueue body (invalid JSON, a
#: non-object root, a member of the wrong type). With the DLQ on it is dead-lettered directly and never
#: retried, and the DLQ record is the *wrapped* poison body built by
#: :meth:`KafkaTransport._poison_body` (``dead_letter.reason == "poison"``), not the raw bytes. With
#: the DLQ off the runtime drops it (DELETE) or retries it up to ``max_attempts`` (Go ``routePoison``).
POISON_ROUTE = UNSUPPORTED_SCHEMA_ROUTE_PREFIX + "poison"


#: How long (seconds) a settle-path publish waits for the broker to confirm the write.
DEFAULT_DELIVERY_TIMEOUT = 30.0

#: The RECOMMENDED retry-topic tiers (§6.4), in milliseconds: 5 s, 30 s, 5 min, 30 min. Opt-in: a
#: transport created without ``retry_tiers_ms`` has no tiers and ``publish_delayed`` / retry raise.
DEFAULT_RETRY_TIERS_MS = [5000, 30000, 300000, 1800000]

#: Record headers the SDK adds to a record parked on a retry topic (§6.4/§6.5). The re-inject
#: consumer removes the first two when it moves the record back to its work topic.
DELAY_HEADER = "bq-delay"
ORIGINAL_TOPIC_HEADER = "bq-original-topic"

Headers = List[Tuple[str, bytes]]


class KafkaDeliveryError(BabelQueueError):
    """A retry / dead-letter write was not confirmed by the broker (delivery error or timeout)."""


def _runtime_attempts(env: Any) -> int:
    """The envelope's ``attempts`` exactly as the runtime's retry path reads it
    (``int(envelope.get("attempts", 0))``): an absent key or a null is 0 (Go decodes a null integer
    as zero). Raises ``ValueError`` for every value that read would choke on — ``""``, a list/dict,
    ``"three"``, ``inf``/``nan`` — and for a bool, which is no counter at all although ``int()``
    happens to accept it."""
    value = env.get("attempts", 0)
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError("attempts is a bool")
    try:
        return int(value)
    except (TypeError, OverflowError) as exc:
        raise ValueError("attempts is not an integer") from exc


def retry_topic(work_topic: str, n: int) -> str:
    """The retry topic of tier ``n`` (1-based) for ``work_topic``: ``<work_topic>.retry.<n>`` (§6.4).
    ``n`` is the tier index, never a delay value."""
    return "%s.retry.%d" % (work_topic, n)


def _validated_tiers(tiers: Optional[Sequence[int]]) -> List[int]:
    """``retry_tiers_ms`` as a list: positive integers (milliseconds) in strictly ascending order."""
    if tiers is None:
        return []
    out: List[int] = []
    for tier in tiers:
        if isinstance(tier, bool) or not isinstance(tier, int) or tier <= 0:
            raise ValueError("retry_tiers_ms must hold positive integer milliseconds, got %r" % (tier,))
        if out and tier <= out[-1]:
            raise ValueError("retry_tiers_ms must be strictly ascending, got %r" % (list(tiers),))
        out.append(tier)
    return out


def _brokers_from_url(url: str) -> str:
    rest = url.split("://", 1)[1] if "://" in url else url
    return rest.split("/", 1)[0] or "localhost:9092"


class KafkaTransport(Transport):
    def __init__(
        self,
        url: str = "kafka://localhost:9092",
        *,
        producer: Any = None,
        consumer_factory: Optional[Callable[[str], Any]] = None,
        group_id: str = "babelqueue",
        delivery_timeout: float = DEFAULT_DELIVERY_TIMEOUT,
        retry_tiers_ms: Optional[Sequence[int]] = None,
        **client_config: Any,
    ) -> None:
        # ``retry_tiers_ms`` is an explicit parameter on purpose: it is a BabelQueue setting, not
        # a librdkafka one, and must never leak into ``client_config`` (which goes to the client).
        self._retry_tiers_ms = _validated_tiers(retry_tiers_ms)
        self._brokers = _brokers_from_url(url or "kafka://localhost:9092")
        self._group_id = group_id
        self._delivery_timeout = delivery_timeout
        self._client_config = client_config
        self._producer = producer
        self._consumer_factory = consumer_factory
        self._consumers: Dict[str, Any] = {}
        # Per queue: the records handed out that have been neither acked nor rewound yet (one for
        # ``pop``; several for ``pop_batch``, which a drain-first consumer such as ``redrive`` uses).
        self._pending: Dict[str, List[Any]] = {}
        # Per queue: records acked while an earlier record on the same partition was still in
        # flight. Kafka's commit is a cumulative per-partition position, so committing them now
        # would pass the unsettled record; they are committed once that record settles.
        self._settled: Dict[str, List[Any]] = {}

    # -- helpers ------------------------------------------------------------

    def _producer_(self) -> Any:
        if self._producer is None:
            self._producer = self._build_producer()  # pragma: no cover - needs Kafka / network
        return self._producer

    def _build_producer(self) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": self._brokers, **self._client_config})

    def _consumer(self, queue: str) -> Any:
        consumer = self._consumers.get(queue)
        if consumer is None:
            if self._consumer_factory is not None:
                consumer = self._consumer_factory(queue)
            else:
                consumer = self._build_consumer(queue)  # pragma: no cover - needs Kafka / network
            self._consumers[queue] = consumer
        return consumer

    def _build_consumer(self, queue: str) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": self._brokers,
                "group.id": self._group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                **self._client_config,
            }
        )
        consumer.subscribe([queue])
        return consumer

    @staticmethod
    def _projection(body: str) -> Headers:
        """Native Kafka record headers (UTF-8 byte values) — a redundant, routable view of the
        body: bq-job/bq-trace-id/bq-message-id + bq-schema-version/lang/attempts. §6.3."""
        env = parse_envelope(body)
        if not env:
            return []
        meta = env.get("meta")
        if not isinstance(meta, dict):
            # A wrongly-typed ``meta`` (a poison body keeps its keys when dead-lettered, §6.5) has no
            # header view; it must not fail the write that settles the record.
            meta = {}

        headers: Headers = []

        def add(key: str, value: Any) -> None:
            if value is not None and value != "":
                headers.append((key, str(value).encode("utf-8")))

        add("bq-job", env.get("job"))
        add("bq-trace-id", env.get("trace_id"))
        add("bq-message-id", meta.get("id"))
        if meta.get("schema_version") is not None:
            headers.append(("bq-schema-version", str(meta["schema_version"]).encode("utf-8")))
        add("bq-source-lang", meta.get("lang"))
        try:
            headers.append(("bq-attempts", str(_runtime_attempts(env)).encode("utf-8")))
        except ValueError:
            # A foreign producer's non-numeric ``attempts``: leave the redundant header out (the
            # body is authoritative then) rather than fail the publish and strand the record.
            pass
        return headers

    @staticmethod
    def _reconcile(body: str, headers: Any) -> str:
        """Set attempts to the authoritative bq-attempts header (falling back to the body's own
        attempts when the header is absent/unparseable — a non-BabelQueue producer). §6.5."""
        env = EnvelopeCodec.decode_lenient(body)
        if not env:
            return body
        try:
            body_attempts = _runtime_attempts(env)
        except ValueError:
            # A foreign producer wrote a non-numeric ``attempts``: there is nothing to reconcile
            # against, and raising out of ``pop`` would skip the record unrestored. Pass the body
            # through untouched; the runtime / ``redrive`` handle it like any other record.
            return body
        attempts = body_attempts
        for key, value in headers or []:
            if key == "bq-attempts":
                # ``replace``: a non-UTF-8 header value must not raise out of ``pop``; it simply
                # fails ``int()`` below and the body's own attempts stand.
                raw = (
                    bytes(value).decode("utf-8", "replace")
                    if isinstance(value, (bytes, bytearray))
                    else str(value)
                )
                try:
                    attempts = int(raw)
                except (ValueError, TypeError):
                    pass
                break
        if attempts == body_attempts and env.get("attempts", 0) is not None:
            return body
        env["attempts"] = attempts
        return EnvelopeCodec.encode(env)

    @staticmethod
    def _unsupported_schema_version(headers: Any) -> Optional[str]:
        """The §6.7 step-2 gate on the ``bq-schema-version`` header: the offending raw value, or
        ``None``.

        ``headers`` is confluent-kafka's ``[(key, value_bytes)]`` list (or ``None``); the first
        ``bq-schema-version`` entry wins and its value is decoded as UTF-8. A missing header, a
        null value or a blank value (the empty string or only ASCII whitespace — the same set in
        every SDK, GR-5, not ``str.strip()``) is not gated: the usual decode path runs. A present
        value is accepted only when it is exactly the core's supported ``SCHEMA_VERSION`` as a
        plain decimal string, so ``"2"``, ``"x"``, ``"01"`` and ``" 1"`` are all rejected."""
        raw: Any = None
        for key, value in headers or []:
            name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
            if name == "bq-schema-version":
                raw = value
                break
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8", "replace")
        raw = str(raw)
        if _BLANK_ASCII_PATTERN.fullmatch(raw) is not None or raw == str(SCHEMA_VERSION):
            return None
        return raw

    @staticmethod
    def _payload(message: Any) -> str:
        """The record value as text. Never raises: bytes that are not UTF-8 are kept as
        ``surrogateescape`` code points, so :meth:`publish` can write the exact bytes back."""
        value = message.value()
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).decode("utf-8", "surrogateescape")
        return str(value) if value is not None else ""

    @staticmethod
    def _key(message: Any) -> Optional[bytes]:
        """The record key as the client reports it (``None`` = null key); tolerant of an injected
        consumer whose records expose no ``key()``."""
        key_of = getattr(message, "key", None)
        return key_of() if callable(key_of) else None

    @staticmethod
    def _decode_failure(text: str) -> Optional[str]:
        """Why ``text`` is not a decodable BabelQueue body, or ``None`` when it is.

        The core decoder is lenient (``decode`` returns ``{}`` for a malformed or non-object body),
        so poison is detected explicitly, with the trigger set of the Go reference's typed
        ``Decode`` but case-sensitive member names, as the core decoder reads them (see :mod:`babelqueue._envelope_shape`). A well-typed but non-conformant envelope
        is not poison: it is routed by its URN, as in Go.

        The shape scan keeps unknown members out of its way, but the core decoder parses the whole
        body with plain ``json.loads`` and turns a ``ValueError`` into an EMPTY envelope (on Python
        3.11+, e.g. an unknown member holding an integer of more than 4300 digits). Such a body is
        poison here too, so content is never silently dropped on the way to a handler."""
        violation = envelope_violation(text)
        if violation is not None:
            return violation
        try:
            json.loads(text)
        except ValueError as exc:
            return "the core decoder cannot parse the body: " + str(exc)
        return None

    @staticmethod
    def _header_value(headers: Any, wanted: str) -> Optional[str]:
        """The text of the first record header named ``wanted`` (``None`` when absent)."""
        for key, value in headers or []:
            name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
            if name != wanted:
                continue
            if isinstance(value, (bytes, bytearray)):
                return bytes(value).decode("utf-8", "replace")
            return None if value is None else str(value)
        return None

    @classmethod
    def _header_attempts(cls, headers: Any) -> int:
        """The ``bq-attempts`` header as a non-negative int (0 when absent or unparseable)."""
        raw = cls._header_value(headers, "bq-attempts")
        if raw is None:
            return 0
        try:
            return max(int(raw), 0)
        except (ValueError, TypeError):
            return 0

    @classmethod
    def _poison_body(cls, text: str, cause: str, original_queue: str, attempts: int) -> str:
        """The DLQ body of a poison record (§6.5, ADR-0035): a strict JSON object body is spliced
        byte for byte with a ``dead_letter`` member, anything else is wrapped as ``raw`` (plain text
        when valid UTF-8, standard base64 plus ``raw_encoding: "base64"`` when not; never U+FFFD).
        The ``dead_letter`` block carries ``reason: "poison"``. See :mod:`babelqueue._poison_record`."""
        block = dead_letter.annotate(
            {},
            "poison",
            original_queue,
            attempts,
            error="Undecodable BabelQueue body: " + cause,
            exception=BabelQueueError.__name__,
        )["dead_letter"]
        return build_poison_record(text, block)

    @staticmethod
    def _is_non_utf8(message: Any) -> bool:
        value = message.value()
        if not isinstance(value, (bytes, bytearray)):
            return False
        try:
            bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return True
        return False

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        producer = self._producer_()
        kwargs = self._record_kwargs(body)
        producer.produce(queue, **kwargs)
        producer.poll(0)

    def publish_settle(
        self, queue: str, body: str, route_urn: str = "", key: Optional[bytes] = None
    ) -> None:
        """:class:`~babelqueue.transport.SettlePublisher`: the runtime's settle write (retry
        republish, envelope / poison / gate DLQ, ``redrive`` restore). Unlike :meth:`publish` it
        waits for the broker's delivery report, because the offset may only be committed once the
        write is confirmed; a delivery error or timeout raises :class:`KafkaDeliveryError`.

        ``key`` is the consumed record's key (:attr:`ReceivedMessage.key`): it is written back
        unchanged, so a retry or DLQ record keeps the original key (``None`` stays a null key, an
        empty key stays empty, and no key is ever synthesised). §6."""
        if isinstance(route_urn, PoisonRoute) and queue != route_urn.queue:
            # The runtime dead-letters a poison record (§6.5): the DLQ body is the Go-form wrap.
            body = self._poison_body(
                body, route_urn.cause, route_urn.original_queue, route_urn.attempts
            )
        kwargs = self._record_kwargs(body, raw_ok=True)
        if key is not None:
            kwargs["key"] = key
        self._produce_confirmed(self._producer_(), queue, kwargs)

    @classmethod
    def _record_kwargs(cls, body: str, raw_ok: bool = False) -> Dict[str, Any]:
        """``produce`` arguments for ``body``. With ``raw_ok`` (the settle path only), a body
        carrying ``surrogateescape`` code points (a consumed record whose value was not UTF-8) is
        written back byte-for-byte with no projected headers: it is not a trustworthy envelope,
        and the projection could not encode it. Without it (a user ``publish``) such a body is
        rejected with ``UnicodeEncodeError``, as before."""
        try:
            body.encode("utf-8")
        except UnicodeEncodeError:
            if not raw_ok:
                raise
            # Bytes that were not UTF-8 are carried as ``surrogateescape`` code points and restored
            # byte for byte. A *lone* surrogate (a ``\\ud800`` JSON escape that survived the decode) is
            # not restorable: it becomes U+FFFD, as the Go reference does when it parses the escape.
            # Without this the settle write would raise and the record would be redelivered forever.
            cleaned = _LONE_SURROGATE.sub("\ufffd", body)
            return {"value": cleaned.encode("utf-8", "surrogateescape"), "headers": []}
        kwargs: Dict[str, Any] = {"value": body.encode("utf-8"), "headers": cls._projection(body)}
        meta = parse_envelope(body).get("meta")
        created_at = meta.get("created_at") if isinstance(meta, dict) else None
        if created_at:
            try:
                kwargs["timestamp"] = int(created_at)
            except (ValueError, TypeError, OverflowError):
                # A poison body keeps its keys when wrapped (§6.5), so ``created_at`` can be anything:
                # a record timestamp is optional and must not fail the write that settles it.
                pass
        return kwargs

    def _produce_confirmed(self, producer: Any, queue: str, kwargs: Dict[str, Any]) -> None:
        """Produce and wait for the delivery report; raise :class:`KafkaDeliveryError` when the
        broker reported an error or did not confirm within ``delivery_timeout``."""
        outcome: Dict[str, Any] = {}

        def on_delivery(err: Any, _msg: Any = None) -> None:
            outcome["error"] = err

        producer.produce(queue, on_delivery=on_delivery, **kwargs)
        producer.flush(self._delivery_timeout)
        # ``flush`` returns the count of *every* message still queued on the producer, so it says
        # nothing about this write: only this write's own delivery report decides. No report within
        # the timeout means it is unconfirmed.
        if "error" not in outcome:
            raise KafkaDeliveryError(
                f"Kafka delivery to {queue!r} was not confirmed within {self._delivery_timeout}s."
            )
        if outcome["error"] is not None:
            raise KafkaDeliveryError(f"Kafka delivery to {queue!r} failed: {outcome['error']}")

    # -- delayed delivery and retry tiers (§6.4 / §6.5, ADR-0034) --------------

    @property
    def retry_tiers_ms(self) -> List[int]:
        """The configured retry tiers in milliseconds (empty = tiers off)."""
        return list(self._retry_tiers_ms)

    def _tier_record(
        self, work_topic: str, body: str, n: int, delay_ms: int, bq_attempts: Optional[int] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """The tier topic and ``produce`` arguments for parking ``body`` on tier ``n``: the usual
        projected headers (``bq-attempts`` overridden when ``bq_attempts`` is given), plus
        ``bq-delay`` = ``delay_ms`` and ``bq-original-topic`` = ``work_topic`` (UTF-8 decimal /
        text, §6.3)."""
        kwargs = self._record_kwargs(body)
        headers: Headers = [h for h in kwargs["headers"] if h[0] not in (DELAY_HEADER, ORIGINAL_TOPIC_HEADER)]
        if bq_attempts is not None:
            headers = [h for h in headers if h[0] != "bq-attempts"]
            headers.append(("bq-attempts", str(bq_attempts).encode("utf-8")))
        headers.append((DELAY_HEADER, str(delay_ms).encode("utf-8")))
        headers.append((ORIGINAL_TOPIC_HEADER, work_topic.encode("utf-8")))
        kwargs["headers"] = headers
        return retry_topic(work_topic, n), kwargs

    def publish_delayed(self, queue: str, body: str, delay_ms: int) -> None:
        """Publish ``body`` to ``queue`` so it reaches the work topic no earlier than ``delay_ms``
        from now (the REQUESTED-DELAY rule, §6.4): it is parked on the smallest configured tier
        ``<queue>.retry.<n>`` whose delay is at least ``delay_ms``, with ``bq-delay`` = the
        requested delay and ``bq-original-topic`` = ``queue``. A delay above the largest tier, a
        negative delay, or no tiers configured raises :class:`BabelQueueError` before anything is
        produced (no clamping). The write is confirmed by the broker."""
        if isinstance(delay_ms, bool) or not isinstance(delay_ms, int) or delay_ms < 0:
            raise BabelQueueError("delay_ms must be a non-negative integer, got %r." % (delay_ms,))
        if not self._retry_tiers_ms:
            raise BabelQueueError(
                "Kafka delayed delivery needs retry tiers: pass retry_tiers_ms (e.g. DEFAULT_RETRY_TIERS_MS)."
            )
        for index, tier_ms in enumerate(self._retry_tiers_ms, start=1):
            if tier_ms >= delay_ms:
                topic, kwargs = self._tier_record(queue, body, index, delay_ms)
                self._produce_confirmed(self._producer_(), topic, kwargs)
                return
        raise BabelQueueError(
            "Requested delay %d ms exceeds the largest retry tier (%d ms)." % (delay_ms, self._retry_tiers_ms[-1])
        )

    def redeliver(self, message: ReceivedMessage, body: str, delay: float) -> None:
        """:class:`~babelqueue.transport.Redeliverer`: the retry / release rule (§6.5). The record
        is parked on tier ``n = min(attempts_before, N-1) + 1`` of the ``N`` configured tiers (it
        stays on the last tier once they run out), carrying ``bq-attempts = attempts_before + 1``,
        ``bq-delay`` = the tier's own delay and ``bq-original-topic`` = the work topic, with the
        body ``attempts`` kept in sync. The ``delay`` argument is ignored: Kafka does not apply
        the runtime's ``retry_backoff`` (ADR-0034 §9). The consumed record's key is written back
        byte for byte, the write is confirmed, and only then is the original record acknowledged.
        With no tiers configured this raises :class:`BabelQueueError` and produces nothing (the
        runtime logs it, leaves the record uncommitted, rewinds to it and keeps consuming)."""
        if not self._retry_tiers_ms:
            raise BabelQueueError(
                "Kafka retry needs retry tiers: pass retry_tiers_ms (e.g. DEFAULT_RETRY_TIERS_MS)."
            )
        attempts_before = self._attempts_before(message)
        n = min(attempts_before, len(self._retry_tiers_ms) - 1) + 1
        tier_ms = self._retry_tiers_ms[n - 1]
        next_attempts = attempts_before + 1
        topic, kwargs = self._tier_record(
            message.queue, self._with_attempts(body, next_attempts), n, tier_ms, next_attempts
        )
        if message.key is not None:
            kwargs["key"] = message.key
        self._produce_confirmed(self._producer_(), topic, kwargs)
        try:
            self.ack(message)
        except Exception:  # noqa: BLE001 - the copy is already on the tier topic
            # The retry copy is confirmed; a failed commit only means the broker redelivers the
            # original (at-least-once), exactly as a failed ack of a handled message.
            _logger.error(
                "Kafka commit after parking a retry copy of a record on %r failed; "
                "the record will be redelivered.",
                message.queue,
                exc_info=True,
            )

    @staticmethod
    def _attempts_before(message: ReceivedMessage) -> int:
        """The attempts the consumed record had before this retry (the reconciled ``attempts``)."""
        env = EnvelopeCodec.decode_lenient(message.body)
        try:
            return max(_runtime_attempts(env), 0) if env else 0
        except ValueError:
            return 0

    @staticmethod
    def _with_attempts(body: str, attempts: int) -> str:
        """``body`` with its ``attempts`` set to ``attempts`` (the body is passed through
        untouched when it already says so, or is not an envelope)."""
        env = EnvelopeCodec.decode_lenient(body)
        if not env:
            return body
        try:
            if _runtime_attempts(env) == attempts and env.get("attempts") is not None:
                return body
        except ValueError:
            pass
        env["attempts"] = attempts
        return EnvelopeCodec.encode(env)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        self._rewind_abandoned(queue)
        return self._poll_one(queue, timeout)

    def pop_batch(
        self, queue: str, max: int = 0, timeout: float = 1.0
    ) -> List[ReceivedMessage]:
        """Up to ``max`` records (all currently available when ``max`` is 0), all held in flight
        (:class:`~babelqueue.transport.BatchPopper`).

        A drain-first consumer pops every record before it settles any, so the abandoned-record
        rewind that every :meth:`pop` starts with must run once, up front — not between the polls,
        where it would seek back to the record just handed out and return it again. The records
        are then acked in any order; commits still never pass an unsettled one (:meth:`ack`).
        """
        self._rewind_abandoned(queue)
        batch: List[ReceivedMessage] = []
        while max == 0 or len(batch) < max:
            try:
                received = self._poll_one(queue, timeout)
            except Exception:
                # A record ``pop`` cannot build (e.g. a malformed ``attempts``) is not tracked as in
                # flight (see ``_poll_one``). Raising here would abandon the records already held,
                # and the next call would rewind to them and hit the same record again: the
                # partition could never be drained. Hand out what was taken instead, exactly as the
                # single-record ``pop`` moves past it on its next call. With nothing taken yet there
                # is nothing to lose, so the error surfaces.
                if not batch:
                    raise
                _logger.exception(
                    "Kafka record on %r could not be decoded; ending the batch at %d record(s).",
                    queue,
                    len(batch),
                )
                break
            if received is None:
                break
            batch.append(received)
        return batch

    def _poll_one(self, queue: str, timeout: float) -> Optional[ReceivedMessage]:
        wait = timeout if timeout and timeout > 0 else 1.0
        message = self._consumer(queue).poll(wait)
        if message is None or message.error() is not None:
            return None
        # §6.7 step 2: gate on the ``bq-schema-version`` header BEFORE any body decode (the attempts
        # reconcile below decodes the body). A rejected record keeps its raw payload and the
        # reserved route; the runtime then dead-letters it directly (no retry; §6.5).
        headers = message.headers()
        unsupported = truncate_declared_version(
            self._unsupported_schema_version(headers)
        )
        if unsupported is not None:
            _logger.warning(
                "Kafka record on %r carries unsupported bq-schema-version %r (supported: %s); "
                "not decoding it.",
                queue,
                unsupported,
                SCHEMA_VERSION,
            )
            received = ReceivedMessage(
                body=self._payload(message),
                queue=queue,
                handle=message,
                route_urn=UNSUPPORTED_SCHEMA_ROUTE_PREFIX + quote(unsupported, safe=""),
                key=self._key(message),
            )
            self._pending.setdefault(queue, []).append(message)
            return received
        payload = self._payload(message)
        non_utf8 = self._is_non_utf8(message)
        cause: Optional[str]
        if non_utf8:
            cause = "the value is not valid UTF-8"
        else:
            try:
                cause = self._decode_failure(payload)
            except Exception as exc:  # noqa: BLE001 - e.g. RecursionError on a pathologically nested body
                cause = "the body could not be scanned (%s)" % type(exc).__name__
        if cause is None:
            try:
                # ADR-0032 section 6 / conformance ``kafka_poison``: a readable body that names no valid
                # ``job`` or ``urn`` can never reach a handler, so on Kafka it shares the poison fate
                # (DLQ ``reason: poison``, no handler call) instead of looping through the FAIL path.
                if not EnvelopeCodec.urn(EnvelopeCodec.decode_lenient(payload)):
                    cause = "the body carries no valid 'job' or 'urn' identity (ADR-0032 section 6)"
            except Exception as exc:  # noqa: BLE001 - never wedge a partition on a body that cannot be classified
                cause = "the body could not be classified (%s)" % type(exc).__name__
        body = payload
        if cause is None:
            try:
                body = self._reconcile(payload, headers)
            except Exception as exc:  # noqa: BLE001 - any decode failure is poison, never a wedged partition
                cause = "the body could not be decoded (%s)" % type(exc).__name__
        if cause is not None:
            # §6.5 poison: there is no envelope to hand to a handler, so the record is handed over, raw,
            # under a reserved route; the runtime dead-letters it directly when the DLQ is on, and
            # otherwise drops it (DELETE) or retries it through the bounded FAIL path. The route
            # carries what ``publish_settle`` needs to wrap the DLQ body like the Go reference (``dead_letter.reason == "poison"``, the original key); a
            # restore to the queue the record was read from (``redrive``) stays byte for byte.
            _logger.warning("Kafka record on %r is poison (%s); settling it per the DLQ / on_unknown_urn configuration.", queue, cause)
            base = NON_UTF8_ROUTE if non_utf8 else POISON_ROUTE
            received = ReceivedMessage(
                body=payload,
                queue=queue,
                handle=message,
                route_urn=PoisonRoute(
                    base,
                    queue,
                    self._header_value(headers, "bq-original-topic") or queue,
                    self._header_attempts(headers),
                    cause,
                ),
                key=self._key(message),
            )
            self._pending.setdefault(queue, []).append(message)
            return received
        received = ReceivedMessage(body=body, queue=queue, handle=message, key=self._key(message))
        # Track the record as in flight only once ``pop`` has built it. Any decode error above is
        # routed as poison instead of escaping: an escape would leave the record uncommitted, so the
        # next ``pop`` would seek back to it and fail identically, wedging the partition forever.
        self._pending.setdefault(queue, []).append(message)
        return received

    @staticmethod
    def _partition_of(handle: Any) -> Tuple[Any, Any]:
        # A handle without a position (a stand-in that is not a Kafka record) is its own bucket.
        try:
            return (handle.topic(), handle.partition())
        except AttributeError:
            return (None, None)

    @staticmethod
    def _offset_of(handle: Any) -> int:
        try:
            return int(handle.offset())
        except AttributeError:
            return -1

    def ack(self, message: ReceivedMessage) -> None:
        handle = message.handle
        if handle is None:
            return
        queue = message.queue
        # Settled: from here on the record is no longer "in flight", whatever the commit does.
        pending = [h for h in self._pending.get(queue, []) if h is not handle]
        if pending:
            self._pending[queue] = pending
        else:
            self._pending.pop(queue, None)
        settled = self._settled.get(queue, []) + [handle]
        # The commit is a cumulative per-partition position: it may only reach as far as the lowest
        # record on that partition that is still in flight.
        partition = self._partition_of(handle)
        floor = min(
            (self._offset_of(h) for h in pending if self._partition_of(h) == partition), default=None
        )
        committable = [
            h
            for h in settled
            if self._partition_of(h) == partition and (floor is None or self._offset_of(h) < floor)
        ]
        remaining = [h for h in settled if not any(h is c for c in committable)]
        if remaining:
            self._settled[queue] = remaining
        else:
            self._settled.pop(queue, None)
        if not committable:
            return
        top = max(committable, key=self._offset_of)
        self._consumer(queue).commit(message=top, asynchronous=False)

    def rewind(self, message: ReceivedMessage) -> None:
        """Seek the partition back to ``message``'s offset so the next ``pop`` returns it again
        (:class:`~babelqueue.transport.Rewinder`). Kafka's commit is a cumulative per-partition
        position, so merely not acking a record would lose it once a later record is committed."""
        handle = message.handle
        if handle is None:
            return
        self._seek_back(message.queue, handle)
        self._forget_from(message.queue, self._partition_of(handle), self._offset_of(handle))

    def _forget_from(self, queue: str, partition: Tuple[Any, Any], offset: int) -> None:
        """Drop the in-flight and deferred-settled records of ``partition`` at or after ``offset``:
        the seek made the broker redeliver them."""
        for book in (self._pending, self._settled):
            kept = [
                h
                for h in book.get(queue, [])
                if self._partition_of(h) != partition or self._offset_of(h) < offset
            ]
            if kept:
                book[queue] = kept
            else:
                book.pop(queue, None)

    def _rewind_abandoned(self, queue: str) -> None:
        """Re-seek the records that were handed out but never acked or rewound.

        That happens when the runtime's settlement raised (a failed retry / DLQ / release
        ``publish``) or a drain-first consumer stopped midway: ``poll`` has already advanced the
        in-memory position, so without this the next ``pop`` (a restarted consume loop) would
        return the *following* record, and acking it would commit past the unsettled one — losing
        it. Seeking back to the lowest such record per partition makes them redeliver instead.
        """
        pending = self._pending.get(queue)
        if not pending:
            return
        lowest: Dict[Tuple[Any, Any], Any] = {}
        for handle in pending:
            partition = self._partition_of(handle)
            if partition not in lowest or self._offset_of(handle) < self._offset_of(lowest[partition]):
                lowest[partition] = handle
        for partition, handle in lowest.items():
            try:
                self._seek_back(queue, handle)
            except Exception:  # noqa: BLE001 - already logged, naming the record
                # A seek can only fail here for a partition that is no longer assigned (rebalance);
                # the group then resets the position to the committed offset, which is that
                # record. Do not wedge the consumer on it.
                pass
            self._forget_from(queue, partition, self._offset_of(handle))
        self._pending.pop(queue, None)

    def _seek_back(self, queue: str, handle: Any) -> None:
        try:
            from confluent_kafka import TopicPartition
        except ImportError:  # only reachable with an injected consumer
            from collections import namedtuple

            TopicPartition = namedtuple("TopicPartition", "topic partition offset")  # type: ignore[misc,no-redef]
        topic, partition, offset = handle.topic(), handle.partition(), handle.offset()
        try:
            self._consumer(queue).seek(TopicPartition(topic, partition, offset))
        except Exception:
            # Name the record so an operator can recover it by hand: if the seek fails the
            # commit of a later record on this partition may skip it.
            _logger.error(
                "Kafka seek back to %s[%s]@%s failed; the unsettled record may be skipped by a "
                "later commit on this partition.",
                topic,
                partition,
                offset,
            )
            raise

    def close(self) -> None:  # pragma: no cover - resource cleanup
        try:
            if self._producer is not None:
                self._producer.flush()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
