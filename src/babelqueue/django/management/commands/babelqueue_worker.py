from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from babelqueue.django import get_app


class Command(BaseCommand):
    help = "Run the BabelQueue consumer: routes inbound polyglot messages by URN."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--queue",
            dest="queue",
            default=None,
            help="Queue to consume (default: the configured queue).",
        )
        parser.add_argument(
            "--max-messages",
            dest="max_messages",
            type=int,
            default=None,
            help="Stop after N messages (default: run until interrupted).",
        )
        parser.add_argument(
            "--timeout",
            dest="timeout",
            type=float,
            default=1.0,
            help="Per-poll block timeout in seconds (default: 1.0).",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        app = get_app()
        queue = options["queue"] or app.queue
        self.stdout.write(f"BabelQueue consumer listening on '{queue}' …")
        processed = app.consume(
            options["queue"],
            max_messages=options["max_messages"],
            timeout=options["timeout"],
        )
        self.stdout.write(self.style.SUCCESS(f"Processed {processed} message(s)."))
