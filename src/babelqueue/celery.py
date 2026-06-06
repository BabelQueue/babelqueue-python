"""Celery integration. Requires the ``celery`` extra:

    pip install "babelqueue[celery]"

A Celery app already configures a broker (Redis/RabbitMQ). :func:`from_celery`
builds a :class:`~babelqueue.BabelQueue` runtime on that *same* broker, so a
Celery-based service produces and consumes the canonical polyglot envelope
alongside its Celery tasks — interoperating with the PHP/Laravel, Go, Node, ...
SDKs. :func:`install_worker` runs that consumer as a Celery worker *bootstep* (a
daemon thread started on ``celery worker``), so one process handles both Celery
tasks and inbound polyglot messages.

``celery`` is imported lazily, so the core stays dependency-free.
"""

from __future__ import annotations

import threading
from typing import Any, Optional

from .app import BabelQueue
from .exceptions import BabelQueueError


def broker_url(celery_app: Any) -> str:
    """Extract the broker URL from a Celery app (supports old/new config keys)."""
    conf = getattr(celery_app, "conf", None)
    url = None
    if conf is not None:
        url = getattr(conf, "broker_url", None)
        if not url and hasattr(conf, "get"):
            url = conf.get("broker_url") or conf.get("BROKER_URL")
    if not url:
        raise BabelQueueError(
            "The Celery app has no broker configured; set broker_url before calling from_celery()."
        )
    return str(url)


def from_celery(celery_app: Any, **kwargs: Any) -> BabelQueue:
    """Build a :class:`~babelqueue.BabelQueue` runtime on the Celery app's broker.

    Extra keyword arguments are forwarded to ``BabelQueue`` (``queue``,
    ``max_attempts``, ``dead_letter``, ``on_unknown_urn``, ...).
    """
    return BabelQueue(broker_url(celery_app), **kwargs)


def install_worker(
    celery_app: Any,
    babel_app: Optional[BabelQueue] = None,
    *,
    queue: Optional[str] = None,
    **kwargs: Any,
) -> type:
    """Register a Celery worker bootstep that consumes BabelQueue messages.

    When a ``celery worker`` boots, the step starts a daemon thread running the
    BabelQueue consumer loop (URN routing, retry → dead-letter). If ``babel_app``
    is omitted it is built with :func:`from_celery`. Returns the bootstep class.
    """
    from celery import bootsteps  # lazy: only needed for this integration

    app = babel_app if babel_app is not None else from_celery(celery_app, **kwargs)

    class BabelQueueConsumerStep(bootsteps.StartStopStep):
        """Runs the BabelQueue consumer loop alongside Celery's own consumer."""

        def __init__(self, parent: Any, **options: Any) -> None:
            super().__init__(parent, **options)
            self._thread: Optional[threading.Thread] = None
            self._stop = threading.Event()

        def start(self, parent: Any) -> None:
            def loop() -> None:
                while not self._stop.is_set():
                    app.consume(queue, max_messages=1, timeout=1.0)

            self._thread = threading.Thread(
                target=loop, name="babelqueue-consumer", daemon=True
            )
            self._thread.start()

        def stop(self, parent: Any) -> None:
            self._stop.set()

    celery_app.steps["worker"].add(BabelQueueConsumerStep)
    return BabelQueueConsumerStep
