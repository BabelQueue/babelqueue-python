"""Optional Replay-Bypass guard (ADR-0027): skip external side-effects on a deliberate replay.

The Python mirror of the Go ``replay.go``. It closes the loop left open by
:mod:`babelqueue.redrive`: a deliberate replay re-runs the handler, and its external
side-effects re-fire (a second charge, a duplicate email). :mod:`babelqueue.idempotency` stops
an *accidental* duplicate; this stops the *intended* reprocess from re-firing effects that
already happened.

``Redrive`` (with ``bypass=True``) stamps a ``bq-replay-bypass`` transport header on a redriven
message; the runtime surfaces it to the handler via a :class:`contextvars.ContextVar`. A handler
wraps its external, non-idempotent side in :func:`bypass_external_effects` so a replay re-runs the
idempotent core but skips effects that already fired::

    from babelqueue import is_replay, bypass_external_effects

    @app.handler("urn:babel:orders:created")
    def on_order_created(data, meta):
        save_order(data)                                   # idempotent core — always runs
        bypass_external_effects(lambda: send_email(data))  # external effect — skipped on replay

The marker rides **out of band** as a transport header, so the frozen envelope is untouched
(GR-1). It propagates over a real broker only once that broker's concrete transport implements
the optional :class:`~babelqueue.transport.HeaderPublisher` capability — a follow-up, like the
broker bindings; the in-memory transport supports it today, so the path is end-to-end testable.
"""

from __future__ import annotations

import contextlib
import contextvars
from typing import Callable, Iterator, Optional, TypeVar

#: The out-of-band transport header :func:`~babelqueue.redrive.redrive` stamps on a replayed
#: message (with ``bypass=True``) and that the runtime surfaces as :func:`is_replay`.
HEADER_REPLAY_BYPASS = "bq-replay-bypass"

_replay_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "babelqueue_replay", default=False
)

T = TypeVar("T")


def is_replay() -> bool:
    """Whether the message currently being handled was redriven with the replay-bypass marker.

    True means this is a deliberate replay, so external side-effects that already happened should
    be skipped. Reads the flag the runtime set on the context from the
    :data:`HEADER_REPLAY_BYPASS` transport header.
    """
    return _replay_var.get()


def bypass_external_effects(fn: Callable[[], T]) -> Optional[T]:
    """Run ``fn`` unless the current message is a replay (see :func:`is_replay`), in which case
    skip it and return ``None``.

    Wrap the external, non-idempotent side of a handler — sending an email, charging a card,
    calling a third party — so a replay re-runs the idempotent core but does not re-fire effects
    that already happened.
    """
    if is_replay():
        return None
    return fn()


@contextlib.contextmanager
def _replay_scope(active: bool) -> Iterator[None]:
    """Internal: mark the current context as a replay (or not) for the span of one dispatch."""
    token = _replay_var.set(active)
    try:
        yield
    finally:
        _replay_var.reset(token)
