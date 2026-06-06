"""Typed, duck-typing-friendly contracts for polyglot messages.

These mirror the other SDKs' contracts so a typed message class can be checked,
but the codec also accepts plain ``(urn, data)`` — Python does not require a class.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class PolyglotMessage(Protocol):
    """A producible message: a stable URN plus a pure, JSON-serialisable payload."""

    def get_babel_urn(self) -> str:
        """The message URN (e.g. ``urn:babel:orders:created``), never a class name."""
        ...

    def to_payload(self) -> Mapping[str, Any]:
        """The pure, JSON-serialisable payload carried under the envelope ``data``."""
        ...


@runtime_checkable
class HasTraceId(Protocol):
    """Optional: lets a message continue an existing distributed trace."""

    def get_babel_trace_id(self) -> Optional[str]:
        """An inherited trace id to reuse, or ``None`` to mint a new one."""
        ...
