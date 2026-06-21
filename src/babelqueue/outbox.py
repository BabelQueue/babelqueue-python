"""Optional transactional-outbox helper (ADR-0029): atomic write + relayed publish.

The Python mirror of the PHP ``BabelQueue\\Outbox`` helper. It removes the producer
**dual write** — "commit the business row" *and* "publish to the broker" are two systems
that can disagree on a crash. Instead the message is written **into the same database, in
the same transaction** as the business data (so it commits or rolls back atomically with
it), and a separate :class:`OutboxRelay` publishes the durable rows afterwards. No
distributed transaction; exactly-once *handoff* into the broker, then at-least-once on the
wire as always (the consumer dedupes on ``meta.id`` — :mod:`babelqueue.idempotency`,
ADR-0022, is the consumer-side mirror of this producer-side helper).

    from babelqueue import EnvelopeCodec
    from babelqueue.outbox import Outbox, OutboxRelay, InMemoryOutboxStore

    store = InMemoryOutboxStore()          # production: a DB-backed OutboxStore adapter
    outbox = Outbox(store)

    # write side — the CALLER owns the transaction boundary (this is the whole point):
    with db.transaction():                 # the caller's own open transaction
        db.insert_order(order)             # the business write
        envelope = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042})
        outbox.write(envelope)             # same connection, same tx — commits or rolls back together

    # read/publish side — run on a short interval, after the business tx commits:
    relay = OutboxRelay(app.transport, store)
    relay.drain()                          # publish all pending rows through the Transport

The helper is intentionally tiny and **stdlib-only** (GR-7): :class:`OutboxStore` is an
abstract persistence contract the caller binds to their own DB — the core ships only the
in-memory :class:`InMemoryOutboxStore` reference and pulls in **no** DB driver. The stored
value is the :class:`~babelqueue.codec.EnvelopeCodec`-encoded envelope, **byte-for-byte
unchanged** (GR-1): the relay publishes exactly those bytes — it never decodes, rebuilds or
re-encodes — so ``trace_id`` is preserved end-to-end (GR-4) and the body is byte-compatible
across SDKs (GR-5). The outbox's own columns (id, queue, attempts) are bookkeeping *around*
the envelope, never a field *on* the wire.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Protocol, runtime_checkable

from .codec import EnvelopeCodec
from .transport import Transport

#: Sleep for the given number of seconds (the relay's backoff seam; default :func:`time.sleep`).
Sleeper = Callable[[float], None]


@dataclass
class OutboxRecord:
    """One pending row read back from an :class:`OutboxStore` for the :class:`OutboxRelay`.

    It pairs the store's own bookkeeping (``id``, ``attempts``) with the verbatim, frozen
    wire envelope (``body``) and the queue it should go to. ``body`` is the exact
    :meth:`~babelqueue.codec.EnvelopeCodec.encode` output handed to :meth:`OutboxStore.save`;
    the relay publishes these bytes unchanged (GR-1/GR-5), so ``trace_id`` is preserved
    end-to-end (GR-4) without the relay ever decoding or rebuilding the envelope.
    """

    id: str  #: The outbox row id (the store's primary key, not ``meta.id``).
    body: str  #: The frozen, encoded envelope JSON, byte-for-byte as stored.
    queue: str  #: The logical queue the relay should publish to.
    attempts: int = 0  #: How many times the relay has already tried to publish this row.


@dataclass
class OutboxRelayResult:
    """Summary of one :meth:`OutboxRelay.flush` pass (or a whole :meth:`OutboxRelay.drain`):
    how many pending rows were published and how many failed (and were left pending for a
    later retry)."""

    published: int = 0
    failed: int = 0

    @property
    def attempted(self) -> int:
        """Total rows the relay attempted in this pass."""
        return self.published + self.failed


@runtime_checkable
class OutboxStore(Protocol):
    """The persistence seam for the transactional outbox (ADR-0029) — the durable "outbox"
    table that an :class:`Outbox` writer fills and an :class:`OutboxRelay` drains.

    **The transaction boundary is the CALLER'S.** The core never opens, commits or rolls
    back anything: :meth:`save` is invoked from *inside* a transaction the caller already
    began (around its own ``INSERT INTO orders …``), and the caller commits both together.
    This keeps the core free of any DB driver (GR-7): the core defines this contract; a
    concrete adapter binds it to a real connection. The reference :class:`InMemoryOutboxStore`
    is for tests and single-process demos.

    The stored value is the **frozen wire envelope, byte-for-byte unchanged** (GR-1): an
    :meth:`~babelqueue.codec.EnvelopeCodec.encode` JSON string. The outbox adds its own
    bookkeeping columns (id, queue, attempts, status) *around* the envelope; it never adds a
    field *to* it. What the relay publishes is the same bytes that were stored.
    """

    def save(self, encoded: str, queue: str) -> str:
        """Persist one encoded envelope into the outbox, **within the transaction the caller
        has already opened** around its business write. Return the new row's outbox id (the
        store's own primary key, NOT ``meta.id``), which the caller may keep for correlation.
        The body is stored verbatim; do not re-encode or mutate it."""
        ...

    def fetch_unpublished(self, limit: int) -> List[OutboxRecord]:
        """Reserve up to ``limit`` rows that are pending publish, **oldest first**, so a relay
        can forward them. Implementations SHOULD lock/claim the rows they return (e.g.
        ``SELECT … FOR UPDATE SKIP LOCKED``, or a ``picked_at`` claim) so two concurrent relays
        do not both publish the same row; at-least-once still tolerates a rare double send.
        Return an empty list when the outbox is drained."""
        ...

    def mark_published(self, ids: List[str]) -> None:
        """Mark the given outbox rows as successfully published (so they are never relayed
        again). Called by the relay only **after** the transport accepted the message."""
        ...

    def mark_failed(self, id: str, error: str) -> None:
        """Record a failed publish attempt for one row: increment its attempt counter and
        store the last error, leaving it pending so a later relay pass retries it
        (at-least-once). The store MAY move a row past a max-attempts threshold to a
        terminal/parked state, but that policy is the adapter's, not the core's. ``error`` is
        a short, human-readable failure reason (never secrets)."""
        ...


class Outbox:
    """The **write side** of the transactional outbox (ADR-0029): turn a BabelQueue envelope
    into a stored outbox row, so the message is persisted *atomically with the business data*
    and a separate :class:`OutboxRelay` publishes it later.

    Usage — the caller owns the transaction boundary (this is the whole point)::

        with db.transaction():             # the caller's own open transaction
            db.insert_order(order)         # the business write
            envelope = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042})
            outbox.write(envelope)         # same connection, same tx — both, or neither

    Because both writes share one transaction, a crash can never leave the business row
    committed without its message (the classic dual-write bug) — they commit or roll back
    together. The handoff to the broker becomes a *local* problem the relay solves.

    This helper only encodes via the frozen :class:`~babelqueue.codec.EnvelopeCodec` (GR-1 —
    the envelope bytes are stored unchanged; the outbox never adds an envelope field) and
    delegates persistence to the injected :class:`OutboxStore`, which the caller binds to
    their own DB (GR-7). It does **not** begin/commit anything.
    """

    def __init__(self, store: OutboxStore) -> None:
        self._store = store

    def write(self, envelope: Mapping[str, Any]) -> str:
        """Encode the envelope (frozen codec, bytes unchanged) and persist it via the store,
        inside the transaction the caller has already opened. Return the new outbox row id
        (for the caller's own correlation, if wanted).

        ``envelope`` is a canonical envelope from :meth:`~babelqueue.codec.EnvelopeCodec.make`
        / :meth:`~babelqueue.codec.EnvelopeCodec.from_message`.
        """
        queue = self._queue_of(envelope)
        return self._store.save(EnvelopeCodec.encode(envelope), queue)

    @staticmethod
    def _queue_of(envelope: Mapping[str, Any]) -> str:
        """The logical queue the message targets: its ``meta.queue``, falling back to
        ``"default"``. Captured at write time so the relay can publish to the right queue
        without decoding the body."""
        meta = envelope.get("meta")
        if isinstance(meta, Mapping):
            queue = meta.get("queue")
            if isinstance(queue, str) and queue != "":
                return queue
        return "default"


class OutboxRelay:
    """The **read/publish side** of the transactional outbox (ADR-0029): drain pending rows the
    :class:`Outbox` writer committed and forward each onto the broker through the frozen
    :class:`~babelqueue.transport.Transport` contract, marking every row published or failed.

    Run it on a short interval (a worker loop, a scheduled command) *after* the business
    transaction commits. Because the message was committed atomically with the business data,
    the relay is the only thing standing between "row exists" and "broker has it" — and it only
    ever reads already-durable rows, so it never invents work.

    **Semantics — at-least-once handoff:**

    - A row is marked **published only after** :meth:`~babelqueue.transport.Transport.publish`
      returns; if the process dies between publish and :meth:`OutboxStore.mark_published`, the
      row stays pending and is published **again** on the next pass. That is at-least-once: a
      downstream consumer must dedupe on the canonical ``meta.id`` (:func:`babelqueue.idempotency.wrap`
      is exactly that guard, the consumer-side mirror — ADR-0022).
    - A publish that **raises** is caught, :meth:`OutboxStore.mark_failed` records the error and
      bumps the attempt count, and the row stays pending for a later retry. One poison row never
      blocks the rest of the batch.
    - **``trace_id`` is preserved end-to-end** (GR-4): the relay publishes the stored bytes
      *verbatim* — it never decodes, rebuilds or re-encodes the envelope — so the body that
      reaches the broker is byte-identical to what was stored (GR-1/GR-5).

    **Backoff:** between a failed publish and the next attempt within the same pass the relay
    sleeps for a bounded, linearly-growing delay (capped), to avoid hammering a broker that is
    briefly down. The sleeper is injectable so tests stay instant.
    """

    #: Hard safety ceiling on :meth:`drain` passes when the caller passes ``0``.
    DEFAULT_DRAIN_CEILING = 10000

    def __init__(
        self,
        transport: Transport,
        store: OutboxStore,
        *,
        batch_size: int = 100,
        backoff_step: float = 0.05,
        backoff_cap: float = 5.0,
        sleeper: Sleeper = time.sleep,
    ) -> None:
        """``transport`` is where published rows go (the same publish-only seam every producer
        uses); ``store`` is the outbox to drain. ``batch_size`` is how many rows to reserve and
        publish per :meth:`flush`. ``backoff_step`` (seconds) is the base delay added per prior
        attempt and ``backoff_cap`` (seconds) is the upper bound on a single backoff sleep.
        ``sleeper`` sleeps the given number of seconds (default :func:`time.sleep`); inject a
        no-op or a recorder in tests."""
        self._transport = transport
        self._store = store
        self._batch_size = batch_size
        self._backoff_step = backoff_step
        self._backoff_cap = backoff_cap
        self._sleeper = sleeper

    def flush(self) -> OutboxRelayResult:
        """Publish one batch of pending rows. Each row the transport accepts is marked
        published; each that raises is marked failed (with a backoff before continuing) and left
        pending. Return a per-pass tally. Call it repeatedly (a loop / cron) to drain the outbox;
        :meth:`drain` loops until it is empty."""
        records = self._store.fetch_unpublished(self._batch_size)

        published_ids: List[str] = []
        failed = 0

        for record in records:
            try:
                # Publish the stored bytes verbatim — never decode/rebuild/re-encode (GR-1).
                self._transport.publish(record.queue, record.body)
                published_ids.append(record.id)
            except Exception as exc:  # noqa: BLE001 - one poison row must not abort the batch
                self._store.mark_failed(record.id, self._reason(exc))
                failed += 1
                self._sleep(self._backoff_for(record.attempts))

        if published_ids:
            self._store.mark_published(published_ids)

        return OutboxRelayResult(len(published_ids), failed)

    def drain(self, max_passes: int = 0) -> OutboxRelayResult:
        """Drain the outbox by repeatedly calling :meth:`flush` while each pass keeps making
        progress (publishes at least one row), then return the cumulative tally. The loop stops
        as soon as a pass publishes nothing — the outbox is empty, or only currently failing rows
        remain (those are left pending for a future :meth:`drain` call once the broker recovers).
        ``max_passes`` is a hard safety ceiling so a degenerate store can never spin forever
        (``0`` = a generous internal default)."""
        ceiling = max_passes if max_passes > 0 else self.DEFAULT_DRAIN_CEILING
        published = 0
        failed = 0

        for _pass in range(ceiling):
            result = self.flush()
            published += result.published
            failed += result.failed

            # No progress this pass → drained, or only failing rows remain. Stop.
            if result.published == 0:
                break

        return OutboxRelayResult(published, failed)

    def _backoff_for(self, prior_attempts: int) -> float:
        """The backoff (seconds) for a row that has already failed ``prior_attempts`` times: a
        linear step per attempt, capped. Kept simple and deterministic so the budget is obvious."""
        delay = self._backoff_step * max(1, prior_attempts + 1)
        return min(delay, self._backoff_cap)

    def _sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._sleeper(seconds)

    @staticmethod
    def _reason(exc: BaseException) -> str:
        """A short, safe failure reason from a raised error (type + message, no traceback)."""
        return f"{type(exc).__name__}: {exc}"


class InMemoryOutboxStore:
    """Process-local reference :class:`OutboxStore` backed by a dict — for tests and
    single-process demos. It has **no real transaction**: :meth:`save` just appends, so it
    cannot deliver the atomic-with-the-business-write guarantee a production store gives. Use a
    database-backed adapter in production.

    It still faithfully models the relay contract: rows are pending until :meth:`mark_published`,
    :meth:`fetch_unpublished` returns them oldest-first, and :meth:`mark_failed` bumps the attempt
    count and stores the last error while leaving the row pending for retry.
    """

    def __init__(self) -> None:
        # Insertion order is preserved by dict, so iteration is naturally oldest-first.
        self._rows: Dict[str, Dict[str, Any]] = {}
        self._sequence = 0

    def save(self, encoded: str, queue: str) -> str:
        self._sequence += 1
        # A non-numeric id keeps the key a genuine string and mirrors the PHP reference.
        row_id = f"ob-{self._sequence}"
        self._rows[row_id] = {
            "body": encoded,
            "queue": queue,
            "attempts": 0,
            "published": False,
            "error": "",
        }
        return row_id

    def fetch_unpublished(self, limit: int) -> List[OutboxRecord]:
        records: List[OutboxRecord] = []
        for row_id, row in self._rows.items():
            if row["published"]:
                continue
            records.append(OutboxRecord(row_id, row["body"], row["queue"], row["attempts"]))
            if len(records) >= limit:
                break
        return records

    def mark_published(self, ids: List[str]) -> None:
        for row_id in ids:
            row = self._rows.get(row_id)
            if row is not None:
                row["published"] = True

    def mark_failed(self, id: str, error: str) -> None:
        row = self._rows.get(id)
        if row is not None:
            row["attempts"] += 1
            row["error"] = error

    def pending_count(self) -> int:
        """Test/inspection helper: the number of rows still pending publish."""
        return sum(1 for row in self._rows.values() if not row["published"])

    def attempts_of(self, id: str) -> int:
        """Test/inspection helper: the recorded attempt count for one row (0 if unknown)."""
        row = self._rows.get(id)
        return row["attempts"] if row is not None else 0

    def last_error_of(self, id: str) -> str:
        """Test/inspection helper: the last recorded error for one row ('' if none)."""
        row = self._rows.get(id)
        return row["error"] if row is not None else ""
