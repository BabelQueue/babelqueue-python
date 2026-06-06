"""Django adapter — settings-driven app, publish() shortcut, worker command.

Skips unless ``django`` is installed (``pip install "babelqueue[django]"``). Uses
the ``memory://`` transport, so no external broker is required.
"""

from __future__ import annotations

import unittest

try:
    import django  # noqa: F401

    HAS_DJANGO = True
except ImportError:
    HAS_DJANGO = False


@unittest.skipUnless(HAS_DJANGO, "django is not installed")
class DjangoAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import django
        from django.conf import settings

        if not settings.configured:
            settings.configure(
                INSTALLED_APPS=["babelqueue.django"],
                BABELQUEUE={"broker_url": "memory://", "queue": "orders"},
                LOGGING_CONFIG=None,
            )
            django.setup()

    def setUp(self) -> None:
        from babelqueue.django import reset

        reset()

    def test_get_app_reads_settings(self) -> None:
        from babelqueue import BabelQueue
        from babelqueue.django import get_app

        app = get_app()
        self.assertIsInstance(app, BabelQueue)
        self.assertEqual(app.queue, "orders")

    def test_publish_then_worker_command_processes_the_message(self) -> None:
        from django.core.management import call_command

        from babelqueue.django import get_app, publish

        seen = {}

        @get_app().handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen["data"] = data

        msg_id = publish("urn:babel:orders:created", {"order_id": 9})
        self.assertTrue(msg_id)

        call_command("babelqueue_worker", "--max-messages=1")

        self.assertEqual(seen["data"], {"order_id": 9})


if __name__ == "__main__":
    unittest.main()
