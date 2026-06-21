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


class DecryptError(BabelQueueError):
    """A protected ``x-gdpr-sensitive`` field could not be restored on the consume side (ADR-0030).

    Raised by :func:`babelqueue.gdpr.unprotect` when a marked leaf is a ciphertext string the
    :class:`~babelqueue.gdpr.Cipher` cannot open — a wrong key, a tampered/garbled ciphertext, or a
    value whose decrypted bytes are not the JSON the producer encoded. The Python mirror of the Go
    ``gdpr.ErrDecrypt``: a missing field is skipped (not an error), but an *unreadable* one stops
    ``unprotect`` so the consumer fails the message (retry / dead-letter) rather than handle
    unreadable PII.
    """
