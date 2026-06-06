"""Exception hierarchy for BabelQueue."""

from __future__ import annotations


class BabelQueueError(Exception):
    """Base for every recoverable BabelQueue error (bad config, empty URN, ...)."""


class UnknownUrnError(BabelQueueError):
    """A consumed message carries a URN with no mapped handler (strategy "fail")."""
