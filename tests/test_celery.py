"""Celery adapter — from_celery bridge + worker bootstep.

Skips unless ``celery`` is installed (``pip install "babelqueue[celery]"``). Uses
Celery's ``memory://`` broker, so no external broker is required.
"""

from __future__ import annotations

import unittest

try:
    import celery  # noqa: F401

    HAS_CELERY = True
except ImportError:
    HAS_CELERY = False

from babelqueue import BabelQueue, BabelQueueError


@unittest.skipUnless(HAS_CELERY, "celery is not installed")
class CeleryAdapterTest(unittest.TestCase):
    def _celery_app(self):
        from celery import Celery

        return Celery("test", broker="memory://")

    def test_from_celery_builds_runtime_on_the_celery_broker(self) -> None:
        from babelqueue.celery import from_celery

        app = from_celery(self._celery_app(), queue="orders")
        self.assertIsInstance(app, BabelQueue)
        self.assertEqual(app.queue, "orders")

        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen["data"] = data

        app.publish("urn:babel:orders:created", {"order_id": 1})
        processed = app.consume(max_messages=1)

        self.assertEqual(processed, 1)
        self.assertEqual(seen["data"], {"order_id": 1})

    def test_from_celery_requires_a_broker(self) -> None:
        from celery import Celery

        from babelqueue.celery import from_celery

        app = Celery("test")  # no broker
        app.conf.broker_url = None
        with self.assertRaises(BabelQueueError):
            from_celery(app)

    def test_install_worker_registers_a_bootstep(self) -> None:
        from babelqueue.celery import from_celery, install_worker

        celery_app = self._celery_app()
        babel = from_celery(celery_app)
        step = install_worker(celery_app, babel)

        self.assertIn(step, celery_app.steps["worker"])


if __name__ == "__main__":
    unittest.main()
