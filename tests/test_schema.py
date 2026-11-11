from __future__ import annotations

import json
import logging
import os
import tempfile
import unittest
from decimal import Decimal
from typing import List

from babelqueue.exceptions import InvalidPayloadError
from babelqueue.schema import (
    DirProvider,
    MapProvider,
    SchemaWarning,
    check,
    unsupported_keywords,
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


class UnionTypeTest(unittest.TestCase):
    def test_union_accepts_any_listed_type(self) -> None:
        schema = {"type": ["string", "null"]}
        self.assertIsNone(validate_schema(schema, "abc"))
        self.assertIsNone(validate_schema(schema, None))
        self.assertIsNotNone(validate_schema(schema, 5))
        self.assertIsNotNone(validate_schema(schema, False))

    def test_integer_accepts_integral_floats_and_decimals(self) -> None:
        for value in (1, 1.0, 100.0, Decimal("2"), Decimal("1E+2")):
            self.assertIsNone(validate_schema({"type": "integer"}, value), repr(value))
        for value in (1.5, Decimal("1.5"), True, "1", float("inf")):
            self.assertIsNotNone(validate_schema({"type": "integer"}, value), repr(value))
        self.assertIsNone(validate_schema({"type": ["integer", "null"]}, 1.0))
        self.assertIsNotNone(validate_schema({"type": ["integer", "boolean"]}, "1"))

    def test_keywords_of_the_matched_type_still_apply(self) -> None:
        schema = {"type": ["integer", "null"], "minimum": 0}
        self.assertIsNone(validate_schema(schema, None))
        self.assertIsNotNone(validate_schema(schema, -1))
        obj = {"type": ["object", "null"], "required": ["a"], "properties": {"a": {"type": "string"}}}
        self.assertIsNone(validate_schema(obj, None))
        self.assertIsNone(validate_schema(obj, {"a": "x"}))
        self.assertIn("a", validate_schema(obj, {}) or "")
        self.assertIsNotNone(validate_schema(obj, {"a": 1}))
        text = {"type": ["string", "null"], "minLength": 2, "maxLength": 3}
        self.assertIsNone(validate_schema(text, None))
        self.assertIsNotNone(validate_schema(text, "a"))
        self.assertIsNotNone(validate_schema(text, "abcd"))

    def test_union_of_unknown_names_constrains_nothing(self) -> None:
        self.assertIsNone(validate_schema({"type": ["nope"]}, 1))
        self.assertIsNone(validate_schema({"type": []}, 1))
        self.assertIsNotNone(validate_schema({"type": ["nope", "string"]}, 1))

    def test_length_bounds_truncate_fractional_values_like_go_and_php(self) -> None:
        self.assertIsNone(validate_schema({"type": "string", "minLength": 2.9}, "ab"))
        self.assertIsNotNone(validate_schema({"type": "string", "minLength": 2.9}, "a"))
        self.assertIsNone(validate_schema({"type": "string", "maxLength": 2.9}, "ab"))
        self.assertIsNotNone(validate_schema({"type": "string", "maxLength": 2.9}, "abc"))

    def test_non_finite_length_bounds_are_ignored(self) -> None:
        # A NaN / Infinity bound (reachable through json.loads) never raises and never changes a verdict.
        for keyword in ("minLength", "maxLength"):
            for bound in (float("nan"), float("inf"), float("-inf"), Decimal("NaN"), Decimal("Infinity")):
                schema = {"type": "string", keyword: bound}
                for text in ("", "a", "abc"):
                    self.assertIsNone(validate_schema(schema, text), (keyword, bound, text))

    def test_provider_warnings_are_read_only_copies(self) -> None:
        with self.assertLogs("babelqueue.schema", level=logging.WARNING):
            provider = MapProvider({"urn:a": {"type": "object", "minProperties": 1}})
        with self.assertRaises(AttributeError):
            provider.warnings = {}  # type: ignore[misc]
        provider.warnings["urn:a"].clear()
        provider.warnings.clear()
        self.assertEqual([SchemaWarning("", "minProperties")], provider.warnings["urn:a"])

    def test_max_length_counts_code_points(self) -> None:
        schema = {"type": "string", "maxLength": 3}
        self.assertIsNone(validate_schema(schema, "\U0001F600" * 3))
        self.assertIsNotNone(validate_schema(schema, "\U0001F600" * 4))
        self.assertIsNotNone(validate_schema({"type": "string", "maxLength": 1}, "e\u0301"))


class NumericEqualityTest(unittest.TestCase):
    def test_const_and_enum_compare_numbers_by_value_across_number_types(self) -> None:
        one_like = (1, 1.0, Decimal("1"), Decimal("1.00"))
        for const in one_like:
            for value in one_like:
                self.assertIsNone(validate_schema({"const": const}, value), f"{const!r} vs {value!r}")
        self.assertIsNone(validate_schema({"enum": [1, 2.5]}, Decimal("2.50")))
        self.assertIsNone(validate_schema({"const": 2.5}, Decimal("2.5")))
        self.assertIsNotNone(validate_schema({"const": 1}, 1.5))
        self.assertIsNotNone(validate_schema({"enum": [1, 2.5]}, 3))

    def test_booleans_strings_and_null_stay_apart_from_numbers(self) -> None:
        self.assertIsNotNone(validate_schema({"const": 1}, True))
        self.assertIsNotNone(validate_schema({"const": True}, 1))
        self.assertIsNotNone(validate_schema({"const": False}, 0))
        self.assertIsNotNone(validate_schema({"const": 1}, "1"))
        self.assertIsNotNone(validate_schema({"const": 1}, None))
        self.assertIsNotNone(validate_schema({"const": None}, 0))
        self.assertIsNotNone(validate_schema({"const": None}, ""))
        self.assertIsNone(validate_schema({"const": None}, None))
        self.assertIsNotNone(validate_schema({"enum": [True, None, 0]}, 1))
        self.assertIsNone(validate_schema({"enum": [True, None, 0]}, 0.0))

    def test_equality_is_recursive_and_key_order_free(self) -> None:
        self.assertIsNone(validate_schema({"const": {"a": [1]}}, {"a": [1.0]}))
        self.assertIsNone(validate_schema({"const": {"a": [1]}}, {"a": [Decimal("1")]}))
        self.assertIsNotNone(validate_schema({"const": {"a": [1]}}, {"a": [True]}))
        self.assertIsNotNone(validate_schema({"const": {"a": [1]}}, {"a": [1, 2]}))
        self.assertIsNotNone(validate_schema({"const": {"a": [1]}}, {"a": [1], "b": 1}))
        self.assertIsNotNone(validate_schema({"const": {"a": [1, 2]}}, {"a": [2, 1]}))
        self.assertIsNone(
            validate_schema({"const": {"a": 1, "b": [2, {"c": 3}]}}, {"b": [2.0, {"c": Decimal("3")}], "a": 1.0})
        )
        self.assertIsNone(validate_schema({"enum": [{"n": 1}, {"n": 2}]}, {"n": 1.0}))
        self.assertIsNotNone(validate_schema({"enum": [{"n": 1}, {"n": 2}]}, {"n": "1"}))


class UnsupportedKeywordTest(unittest.TestCase):
    def test_reports_one_warning_per_occurrence_with_pointers(self) -> None:
        schema = {
            "type": "object",
            "pattern": "x",
            "properties": {
                "a/b": {"type": "string", "pattern": "x"},
                "m~n": {"type": "string", "format": "x"},
                "list": {"type": "array", "items": {"type": "integer", "multipleOf": 2}},
            },
            "additionalProperties": {"type": "string", "maxItems": 1},
        }
        self.assertEqual(
            {
                SchemaWarning("", "pattern"),
                SchemaWarning("/properties/a~1b", "pattern"),
                SchemaWarning("/properties/m~0n", "format"),
                SchemaWarning("/properties/list/items", "multipleOf"),
                SchemaWarning("/additionalProperties", "maxItems"),
            },
            set(unsupported_keywords(schema)),
        )
        self.assertEqual(5, len(unsupported_keywords(schema)))

    def test_annotations_and_property_names_never_warn(self) -> None:
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#", "$id": "x", "title": "t",
            "description": "d", "examples": [], "default": {}, "$comment": "c", "x-owner": "billing",
            "type": "object",
            "properties": {
                "title": {"type": "string", "x-gdpr-sensitive": True},
                "pattern": {"type": "string"},
                "format": {"type": "string"},
            },
        }
        self.assertEqual([], unsupported_keywords(schema))

    def test_unsupported_container_is_one_warning_and_not_descended(self) -> None:
        schema = {"type": "object", "allOf": [{"pattern": "x"}], "definitions": {"s": {"format": "x"}}}
        self.assertEqual(
            {SchemaWarning("", "allOf"), SchemaWarning("", "definitions")}, set(unsupported_keywords(schema))
        )

    def test_unsupported_keywords_do_not_change_the_verdict(self) -> None:
        schema = {"type": "string", "pattern": "^a", "format": "email", "maximum": 1}
        self.assertIsNone(validate_schema(schema, "nope"))
        self.assertIsNotNone(validate_schema(schema, 12))

    def test_map_provider_reports_on_registration_through_the_logger(self) -> None:
        urn = "urn:babel:orders:created"
        with self.assertLogs("babelqueue.schema", level=logging.WARNING) as logs:
            provider = MapProvider(
                {urn: {"type": "object", "properties": {"e": {"type": "string", "format": "email"}}},
                 "urn:babel:clean": {"type": "object"}}
            )
        self.assertEqual({urn: [SchemaWarning("/properties/e", "format")]}, provider.warnings)
        self.assertEqual(1, len(logs.output))
        self.assertIn(urn, logs.output[0])
        self.assertIn("format", logs.output[0])

    def test_map_provider_from_json_reports_too(self) -> None:
        with self.assertLogs("babelqueue.schema", level=logging.WARNING):
            provider = MapProvider.from_json({"urn:a": '{"type":"object","minProperties":1}'})
        self.assertEqual([SchemaWarning("", "minProperties")], provider.warnings["urn:a"])

    def test_dir_provider_reports_once_when_the_schema_is_first_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "o.json"), "w", encoding="utf-8") as fh:
                fh.write('{"type":"object","properties":{"n":{"type":"integer","maximum":5}}}')
            manifest = os.path.join(tmp, "registry.json")
            with open(manifest, "w", encoding="utf-8") as fh:
                fh.write('{"schemas":[{"urn":"urn:o","schema":"o.json"}]}')
            provider = DirProvider(manifest)
            self.assertEqual({}, provider.warnings)
            with self.assertLogs("babelqueue.schema", level=logging.WARNING) as logs:
                provider.schema_for("urn:o")
                provider.schema_for("urn:o")  # cached: no second report
            self.assertEqual(1, len(logs.output))
            self.assertEqual({"urn:o": [SchemaWarning("/properties/n", "maximum")]}, provider.warnings)


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
