from __future__ import annotations

from django.apps import AppConfig


class BabelQueueConfig(AppConfig):
    """Django app config for the BabelQueue adapter."""

    name = "babelqueue.django"
    label = "babelqueue"
    verbose_name = "BabelQueue"
