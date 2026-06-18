from __future__ import annotations

import json
import os
import tempfile
import unittest
from typing import List

from babelqueue.exceptions import InvalidPayloadError
from babelqueue.schema import (
    DirProvider,
    MapProvider,
    check,
    validate,
    validate_schema,
    wrap,
)

ORDERS = (
    '{"type":"object","required":["order_id"],'
    '"properties":{"order_id":{"type":"integer"}},"additionalProperties":false}'
)


class ValidatorTest(unittest.TestCase):
    def test_object_required_types_and_additional(self) -> None:
        schema = json.loads(
            '{"type":"object","required":["order_id"],'
            '"properties":{"order_id":{"type":"integer"},"note":{"type":"string","minLength":1}},'
            '"additionalProperties":false}'
        )
        self.assertIsNone(validate_schema(schema, {"order_id": 7}))
        self.assertIsNotNone(validate_schema(schema, {}))
        self.assertIsNotNone(validate_schema(schema, {"order_id": "x"}))
        self.assertIsNotNone(validate_schema(schema, {"order_id": 7, "extra": 1}))
        self.assertIsNotNone(validate_schema(schema, {"order_id": 7, "note": ""}))

    def test_enum_minimum_and_array_items(self) -> None:
        schema = json.loads(
            '{"type":"object","properties":{"status":{"enum":["new","paid"]},'
            '"qty":{"type":"integer","minimum":1},'
            '"tags":{"type":"array","items":{"type":"string"}}}}'
        )
        self.assertIsNone(validate_schema(schema, {"status": "paid", "qty": 2, "tags": ["a", "b"]}))
        self.assertIsNotNone(validate_schema(schema, {"status": "cancelled"}))
        self.assertIsNotNone(validate_schema(schema, {"qty": 0}))
        self.assertIsNotNone(validate_schema(schema, {"tags": ["a", 1]}))

    def test_scalar_types(self) -> None:
        cases = [
            ('{"type":"boolean"}', True, True),
            ('{"type":"boolean"}', "x", False),
            ('{"type":"null"}', None, True),
            ('{"type":"null"}', 1, False),
            ('{"type":"number","minimum":0.5}', 0.6, True),
            ('{"type":"number","minimum":0.5}', 0.4, False),
            ('{"type":"number"}', "x", False),
            ('{"type":"string"}', 5, False),
            ('{"type":"integer"}', 1.0, True),
            ('{"type":"integer"}', 1.5, False),
            ('{"type":"integer"}', True, False),  # bool is not an integer
            ('{"const":"v1"}', "v1", True),
            ('{"const":"v1"}', "v2", False),
        ]
        for src, value, valid in cases:
            schema = json.loads(src)
            violation = validate_schema(schema, value)
            self.assertEqual(valid, violation is None, f"{src} / {value!r}: {violation}")


class ProviderTest(unittest.TestCase):
    def test_map_provider_from_json(self) -> None:
        provider = MapProvider.from_json({"urn:babel:orders:created": ORDERS})
        self.assertIsNotNone(provider.schema_for("urn:babel:orders:created"))
        self.assertIsNone(provider.schema_for("urn:babel:unknown"))

    def test_map_provider_invalid_json(self) -> None:
        with self.assertRaises(ValueError):
            MapProvider.from_json({"u": "not json"})

    def test_dir_provider_reads_registry_lazily(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "schemas"))
            with open(os.path.join(d, "schemas", "orders.json"), "w", encoding="utf-8") as fh:
                fh.write('{"type":"object","required":["order_id"],"properties":{"order_id":{"type":"integer"}}}')
            with open(os.path.join(d, "registry.json"), "w", encoding="utf-8") as fh:
                fh.write('{"schemas":[{"urn":"urn:babel:orders:created","schema":"schemas/orders.json"},{"urn":"","schema":"x"}]}')

            provider = DirProvider(os.path.join(d, "registry.json"))
            for _ in range(2):  # the second call hits the cache
                self.assertIsNotNone(provider.schema_for("urn:babel:orders:created"))
            self.assertIsNone(provider.schema_for("urn:babel:unknown"))

    def test_dir_provider_missing_manifest(self) -> None:
        with self.assertRaises(OSError):
            DirProvider(os.path.join(tempfile.gettempdir(), "nope_registry_xyz.json"))

    def test_dir_provider_missing_schema_file(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "registry.json"), "w", encoding="utf-8") as fh:
                fh.write('{"schemas":[{"urn":"u","schema":"missing.json"}]}')
            provider = DirProvider(os.path.join(d, "registry.json"))
            with self.assertRaises(OSError):
                provider.schema_for("u")


class FacadeTest(unittest.TestCase):
    def _provider(self) -> MapProvider:
        return MapProvider.from_json({"urn:babel:orders:created": ORDERS})

    def test_check_valid_invalid_unregistered(self) -> None:
        provider = self._provider()
        self.assertIsNone(check(provider, "urn:babel:orders:created", {"order_id": 1}))
        self.assertIsNone(check(provider, "urn:babel:unknown", {"x": 1}))  # opt-in
        self.assertIsNotNone(check(provider, "urn:babel:orders:created", {}))

    def test_validate_raises_on_invalid(self) -> None:
        with self.assertRaises(InvalidPayloadError):
            validate(self._provider(), "urn:babel:orders:created", {"order_id": "x"})

    def test_validate_passes_unregistered(self) -> None:
        validate(self._provider(), "urn:babel:unknown", {"anything": True})  # no raise

    def test_wrap_runs_handler_on_valid(self) -> None:
        calls: List[int] = []
        wrapped = wrap(self._provider(), "urn:babel:orders:created", lambda data, meta: calls.append(1))
        wrapped({"order_id": 1}, {"id": "m1"})
        self.assertEqual(calls, [1])

    def test_wrap_raises_and_skips_on_invalid(self) -> None:
        calls: List[int] = []
        wrapped = wrap(self._provider(), "urn:babel:orders:created", lambda data, meta: calls.append(1))
        with self.assertRaises(InvalidPayloadError):
            wrapped({}, {"id": "m1"})
        self.assertEqual(calls, [])

    def test_wrap_runs_handler_for_unregistered_urn(self) -> None:
        calls: List[int] = []
        wrapped = wrap(self._provider(), "urn:babel:unknown", lambda data, meta: calls.append(1))
        wrapped({"anything": True}, {"id": "m1"})
        self.assertEqual(calls, [1])


if __name__ == "__main__":
    unittest.main()
