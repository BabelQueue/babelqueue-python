"""Builds the additive ``dead_letter`` block attached when a message is
dead-lettered. Pure: returns an annotated copy; the original identity
(trace_id, meta.id, data) is preserved. Because the field is additive and
optional, the envelope stays at schema_version 1. See https://babelqueue.com.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Mapping, Optional

from .codec import SOURCE_LANG


def annotate(
    envelope: Mapping[str, Any],
    reason: str,
    original_queue: str,
    attempts: int,
    *,
    error: Optional[str] = None,
    exception: Optional[str] = None,
    lang: str = SOURCE_LANG,
) -> Dict[str, Any]:
    """Return a copy of ``envelope`` with a ``dead_letter`` block.

    ``reason`` is one of ``failed`` | ``unknown_urn`` | ``poison``.
    """
    result = dict(envelope)
    result["dead_letter"] = {
        "reason": reason,
        "error": error,
        "exception": exception,
        "failed_at": int(time.time() * 1000),
        "original_queue": original_queue,
        "attempts": attempts,
        "lang": lang,
    }
    return result
