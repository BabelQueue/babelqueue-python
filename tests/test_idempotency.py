from __future__ import annotations

import unittest
from typing import Any, Dict, List

from babelqueue.app import _handler_wants_envelope
from babelqueue.idempotency import InMemoryStore, wrap


class IdempotencyTest(unittest.TestCase):
    def test_runs_and_remembers_on_first_delivery(self) -> None:
        store = InMemoryStore()
        calls: List[str] = []

        def handler(data: Dict[str, Any], meta: Dict[str, Any]) -> None:
            calls.append(meta["id"])

        wrapped = wrap(store, handler)
        wrapped({}, {"id": "m1"})

        self.assertEqual(calls, ["m1"])
        self.assertTrue(store.seen("m1"))

    def test_skips_redelivery_of_same_id(self) -> None:
        store = InMemoryStore()
        calls: List[int] = []

        wrapped = wrap(store, lambda data, meta: calls.append(1))
        wrapped({}, {"id": "m1"})
        wrapped({}, {"id": "m1"})  # redelivery → skipped

        self.assertEqual(len(calls), 1)

    def test_runs_again_for_a_different_id(self) -> None:
        store = InMemoryStore()
        calls: List[int] = []

        wrapped = wrap(store, lambda data, meta: calls.append(1))
        wrapped({}, {"id": "m1"})
        wrapped({}, {"id": "m2"})

        self.assertEqual(len(calls), 2)

    def test_does_not_remember_when_handler_raises(self) -> None:
        store = InMemoryStore()
        calls: List[int] = []

        def boom(data: Dict[str, Any], meta: Dict[str, Any]) -> None:
            calls.append(1)
            raise RuntimeError("boom")

        wrapped = wrap(store, boom)
        with self.assertRaises(RuntimeError):
            wrapped({}, {"id": "m1"})
        self.assertFalse(store.seen("m1"))

        # A redelivery runs the handler again — retry works.
        with self.assertRaises(RuntimeError):
            wrapped({}, {"id": "m1"})
        self.assertEqual(len(calls), 2)

    def test_runs_when_no_usable_id(self) -> None:
        store = InMemoryStore()
        calls: List[int] = []

        wrapped = wrap(store, lambda data, meta: calls.append(1))
        wrapped({}, {"id": ""})  # empty id → cannot dedupe → runs
        wrapped({}, {})  # no id at all → runs

        self.assertEqual(len(calls), 2)

    def test_preserves_handler_arity_for_the_runtime(self) -> None:
        # functools.wraps keeps inspect.signature transparent, so the runtime still
        # passes the wrapped handler the right number of positional args.
        store = InMemoryStore()
        self.assertTrue(_handler_wants_envelope(wrap(store, lambda d, m, e: None)))
        self.assertFalse(_handler_wants_envelope(wrap(store, lambda d, m: None)))

    def test_forget_removes_a_remembered_id(self) -> None:
        store = InMemoryStore()
        store.remember("m1")
        self.assertTrue(store.seen("m1"))

        store.forget("m1")
        self.assertFalse(store.seen("m1"))


if __name__ == "__main__":
    unittest.main()
