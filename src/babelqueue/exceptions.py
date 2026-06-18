"""Exception hierarchy for BabelQueue."""

from __future__ import annotations


class BabelQueueError(Exception):
    """Base for every recoverable BabelQueue error (bad config, empty URN, ...)."""


class UnknownUrnError(BabelQueueError):
    """A consumed message carries a URN with no mapped handler (strategy "fail")."""


class InvalidPayloadError(BabelQueueError):
    """A message's ``data`` does not match the JSON Schema registered for its URN (ADR-0024).

    Raised by the producer-side :func:`babelqueue.schema.validate` and the consumer-side
    :func:`babelqueue.schema.wrap`; the latter lets the runtime redeliver (and eventually
    dead-letter) a poison message.
    """

    def __init__(self, urn: str, violation: str) -> None:
        super().__init__(f"data for {urn!r} does not match its URN schema: {violation}")
        self.urn = urn
        self.violation = violation
