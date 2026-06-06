"""Django integration. Requires the ``django`` extra:

    pip install "babelqueue[django]"

Add ``"babelqueue.django"`` to ``INSTALLED_APPS`` and configure a ``BABELQUEUE``
settings dict::

    BABELQUEUE = {
        "broker_url": "redis://localhost:6379/0",
        "queue": "orders",
        "max_attempts": 3,
        "dead_letter": True,
    }

Then publish from views/signals with :func:`publish`, register handlers on
:func:`get_app`, and run the consumer with ``python manage.py babelqueue_worker``.
The runtime is the shared :class:`~babelqueue.BabelQueue`, so messages interoperate
with the PHP/Laravel, Go, Node, ... SDKs. ``django`` is imported lazily.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from ..app import BabelQueue

# Keys (besides broker_url) forwarded to the BabelQueue constructor.
_APP_KWARGS = frozenset(
    {
        "queue",
        "on_unknown_urn",
        "max_attempts",
        "dead_letter",
        "dead_letter_queue",
        "dead_letter_suffix",
        "transport",
    }
)

_app: Optional[BabelQueue] = None


def _build() -> BabelQueue:
    from django.conf import settings  # lazy

    raw: Mapping[str, Any] = getattr(settings, "BABELQUEUE", {}) or {}
    kwargs: Dict[str, Any] = {k: v for k, v in raw.items() if k in _APP_KWARGS}
    broker = raw.get("broker_url", "memory://")
    return BabelQueue(broker, **kwargs)


def get_app() -> BabelQueue:
    """Return the process-wide :class:`~babelqueue.BabelQueue`, built from
    ``settings.BABELQUEUE`` on first use."""
    global _app
    if _app is None:
        _app = _build()
    return _app


def publish(urn: str, data: Mapping[str, Any], **kwargs: Any) -> str:
    """Publish a message through the configured app; returns its id (``meta.id``)."""
    return get_app().publish(urn, dict(data), **kwargs)


def reset() -> None:
    """Drop the cached app so the next :func:`get_app` rebuilds it (tests / settings reload)."""
    global _app
    _app = None


__all__ = ["get_app", "publish", "reset"]
