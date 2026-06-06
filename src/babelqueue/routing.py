"""Unknown-URN strategy names, shared with every other SDK."""

from __future__ import annotations


class UnknownUrnStrategy:
    """What a consumer does with a message whose URN has no mapped handler."""

    FAIL = "fail"
    DELETE = "delete"
    RELEASE = "release"
    DEAD_LETTER = "dead_letter"

    ALL = (FAIL, DELETE, RELEASE, DEAD_LETTER)
