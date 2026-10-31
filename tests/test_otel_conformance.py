"""Runs the ``otel`` sections of the shared cross-SDK conformance suite (conformance 1.9.0):
``span-shape`` (ADR-0038), ``trace-id`` (ADR-0038 section 7) and ``metrics`` (ADR-0039).

Every case is driven through the real ``babelqueue.otel`` wrapper with an in-memory span exporter
or metric reader, so a green run proves the wrapper emits exactly the shape the suite pins. The
``traceparent`` section lives in ``test_behaviour_conformance.py``.

These sections are mandatory for a package that carries ``otel`` (ADR-0032 section 2): they need
the ``opentelemetry-api`` and ``opentelemetry-sdk`` packages (the ``[otel]`` extra plus the SDK,
which CI installs), and a runner that skips would be reported as a failure in ``results.json``.
"""

from __future__ import annotations

import json
import math
import unittest
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from test_behaviour_conformance import _retry_once, _run_cases

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.dead_letter import DeadLetterEvent
from babelqueue.transport import InMemoryTransport

SUITE = Path(__file__).parent / "conformance"
MANIFEST = json.loads((SUITE / "manifest.json").read_text(encoding="utf-8"))


def _has_module(name: str) -> bool:
    try:
        return find_spec(name) is not None
    except ModuleNotFoundError:
        return False


_HAS_OTEL_SDK = _has_module("opentelemetry.sdk.trace") and _has_module("opentelemetry.sdk.metrics")

#: The keys of the ADR-0038 section 4 table: anything else under ``babelqueue.*`` must not appear.
_TABLE_KEYS = frozenset(
    {
        "messaging.system",
        "messaging.operation.type",
        "messaging.operation.name",
        "messaging.destination.name",
        "messaging.message.id",
        "messaging.message.conversation_id",
        "messaging.consumer.group.name",
        "babelqueue.urn",
        "babelqueue.schema_version",
        "babelqueue.attempts",
        "error.type",
        "messaging.operation",
        "messaging.babelqueue.attempts",
    }
)

_ASCII_WHITESPACE = " \t\n\v\f\r"


class _RecordingTransport(InMemoryTransport):
    """An in-memory transport that keeps the bodies handed to it and can fail every publish.

    The body is recorded *before* the failure, which is what "the envelope handed to the
    transport" means for ``send-failure-after-envelope-built``."""

    def __init__(self, fail: bool = False) -> None:
        super().__init__()
        self.fail = fail
        self.handed_over: List[str] = []

    def publish(self, queue: str, body: str) -> None:
        self.handed_over.append(body)
        if self.fail:
            raise RuntimeError("the broker rejected the publish")
        super().publish(queue, body)

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        self.handed_over.append(body)
        if self.fail:
            raise RuntimeError("the broker rejected the publish")
        super().publish_with_headers(queue, body, headers)


def _tracing() -> Tuple[Any, Any]:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("conformance"), exporter


def _metering() -> Tuple[Any, Any]:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return provider.get_meter("conformance"), reader


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


# --------------------------------------------------------------------------------------------
# span-shape
# --------------------------------------------------------------------------------------------


def _substitute(value: Any, produced: Mapping[str, Any]) -> Any:
    """Replace the ``$produced.*`` placeholders by the produced envelope's values."""
    if value == "$produced.meta.id":
        return (produced.get("meta") or {}).get("id")
    if value == "$produced.trace_id":
        return produced.get("trace_id")
    if value == "$produced.meta.queue":
        return (produced.get("meta") or {}).get("queue")
    return value


def _run_send(case: Mapping[str, Any], tracer: Any) -> Tuple[Optional[Exception], Mapping[str, Any]]:
    from babelqueue import otel

    transport = _RecordingTransport(fail=case["outcome"] == "error")
    app = BabelQueue(transport=transport)
    data: Dict[str, Any] = {"order_id": 1}
    if case.get("envelope_built") is False:
        data = {"unserializable": object()}  # make succeeds, encode fails: nothing reaches the transport
        transport.fail = False
    error: Optional[Exception] = None
    try:
        otel.publish(
            tracer, app, case["urn"], data, queue=case.get("queue"), system=case.get("system")
        )
    except Exception as exc:  # noqa: BLE001 - the case decides whether this is expected
        error = exc
    if case.get("envelope_built") is False:
        _ensure(not transport.handed_over, "an envelope that failed to build was handed to the transport")
        return error, {}
    _ensure(len(transport.handed_over) == 1, "exactly one envelope must be handed to the transport")
    return error, json.loads(transport.handed_over[0])


def _run_process(case: Mapping[str, Any], tracer: Any) -> Optional[Exception]:
    from babelqueue import otel

    meta: Dict[str, Any] = {"lang": "python", "schema_version": case["schema_version"]}
    if "queue" in case:
        meta["queue"] = case["queue"]
    if "message_id" in case:
        meta["id"] = case["message_id"]
    envelope = {
        "job": case["urn"],
        "trace_id": case["trace_id"],
        "data": {},
        "meta": meta,
        "attempts": case["attempts"],
    }

    def handler(data: Any, meta: Any, envelope: Any) -> None:
        if case["outcome"] == "error":
            raise RuntimeError("the handler failed")

    wrapped = otel.wrap_handler(
        tracer, handler, system=case.get("system"), consumer_group=case.get("consumer_group")
    )
    try:
        wrapped({}, meta, envelope)
    except Exception as exc:  # noqa: BLE001 - the case decides whether this is expected
        return exc
    return None


def _eval_span_shape(case: Mapping[str, Any]) -> None:
    tracer, exporter = _tracing()
    produced: Mapping[str, Any] = {}
    if case["operation"] == "send":
        error, produced = _run_send(case, tracer)
    else:
        error = _run_process(case, tracer)
    expect = case["expect"]
    failing = case["outcome"] == "error"
    _ensure((error is not None) == failing, f"outcome {case['outcome']!r}, raised: {error!r}")

    spans = exporter.get_finished_spans()
    _ensure(len(spans) == 1, f"expected one span, got {[s.name for s in spans]}")
    span = spans[0]
    attributes = dict(span.attributes or {})

    expected_attributes = {k: _substitute(v, produced) for k, v in expect["attributes"].items()}
    optional = expect.get("optional_attributes", [])

    # The name: exact, or one of the pinned alternatives (a destination written or not).
    if "span_name" in expect:
        name = expect["span_name"]
        if name.startswith("send ") and "$produced.meta.queue" in name:
            queue = (produced.get("meta") or {}).get("queue")
            name = "send" if not queue or not queue.strip(_ASCII_WHITESPACE) else "send " + queue
        _ensure(span.name == name, f"span name {span.name!r} != {name!r}")
    else:
        _ensure(span.name in expect["span_name_one_of"], f"span name {span.name!r} not in {expect['span_name_one_of']}")
    _ensure(span.kind.name == expect["span_kind"], f"span kind {span.kind.name} != {expect['span_kind']}")
    _ensure(span.name.split(" ")[0] in ("send", "process"), "the URN must never be in the span name")
    _ensure(case["urn"] not in span.name, "the URN must never be in the span name")

    # Every listed attribute is present with exactly that value (and type).
    for key, value in expected_attributes.items():
        _ensure(key in attributes, f"missing attribute {key}")
        _ensure(
            type(attributes[key]) is type(value) and attributes[key] == value,
            f"{key}: {attributes[key]!r} != {value!r}",
        )
    # An optional attribute may be absent; when present it must equal the case input, and the
    # span name must agree with it.
    for key in optional:
        if key in attributes:
            _ensure(key == "messaging.destination.name", f"unexpected optional attribute {key}")
            _ensure(attributes[key] == case["queue"], f"{key}: {attributes[key]!r} != {case['queue']!r}")
            _ensure(span.name == "send " + case["queue"], "the span name must agree with the destination attribute")
        elif "span_name_one_of" in expect:
            _ensure(span.name == "send", "no destination attribute means the bare span name")
    # No table key outside the case's expectation, and no stray babelqueue.* key.
    allowed = set(expected_attributes) | set(optional)
    for key in attributes:
        if key == "error.type":
            continue  # asserted below: present on error cases only
        if key in _TABLE_KEYS:
            _ensure(key in allowed, f"unexpected table attribute {key}")
        _ensure(
            not key.startswith("babelqueue.") or key in _TABLE_KEYS,
            f"attribute {key} is outside the ADR-0038 table",
        )

    # Status and error.type.
    _ensure(span.status.status_code.name.lower() == expect["status"].lower(), f"status {span.status.status_code.name}")
    if expect["status"] == "error":
        _ensure(expect.get("error_type") == "present", "an error case pins error.type: present")
        error_type = attributes.get("error.type")
        _ensure(isinstance(error_type, str) and error_type != "", f"error.type: {error_type!r}")
        _ensure(span.status.description not in (None, ""), "the ERROR status carries the message")
    else:
        _ensure("error.type" not in attributes, "error.type is written only when the operation failed")


# --------------------------------------------------------------------------------------------
# trace-id
# --------------------------------------------------------------------------------------------


def _raw_envelope(trace_id: str) -> str:
    envelope = EnvelopeCodec.make("urn:acme:a", {"n": 1})
    envelope["trace_id"] = trace_id
    return json.dumps(envelope, ensure_ascii=False)


def _eval_trace_id(case: Mapping[str, Any]) -> None:
    from babelqueue import otel

    mode = case["mode"]
    expect = case["expect"]
    if mode == "map":
        actual = format(otel.trace_id_of(case["trace_id"]), "032x")
        _ensure(actual == expect["trace_id_hex"], f"trace id {actual} != {expect['trace_id_hex']}")
        _ensure(len(actual) == 32 and actual != "0" * 32, "the mapped TraceID must be valid")
    elif mode == "decode":
        decoded = EnvelopeCodec.decode(_raw_envelope(case["trace_id"]))
        _ensure(decoded != {}, "a readable envelope must decode")
        _ensure(decoded["trace_id"] == expect["trace_id"], f"decoded trace_id {decoded['trace_id']!r}")
    elif mode == "reemit":
        out = json.loads(_retry_once(_raw_envelope(case["trace_id"])))
        _ensure(out["trace_id"] == expect["trace_id"], f"re-emitted trace_id {out['trace_id']!r}")
        _ensure(out["attempts"] == 1, "the re-emit increments attempts")
    elif mode == "downstream":
        produced = EnvelopeCodec.decode(
            EnvelopeCodec.encode(EnvelopeCodec.make("urn:acme:b", {}, trace_id=case["trace_id"]))
        )
        _ensure(produced["trace_id"] == expect["trace_id"], f"downstream trace_id {produced['trace_id']!r}")
    elif mode == "make":
        made = EnvelopeCodec.make(case["urn"], {}, trace_id=case["trace_id"])
        _ensure(made["trace_id"] == expect["trace_id"], f"made trace_id {made['trace_id']!r}")
        actual = format(otel.trace_id_of(made["trace_id"]), "032x")
        _ensure(actual == expect["trace_id_hex"], f"trace id {actual} != {expect['trace_id_hex']}")
    else:  # pragma: no cover - guards against a new, unhandled mode
        raise AssertionError(f"unknown trace-id mode {mode!r}")


# --------------------------------------------------------------------------------------------
# metrics
# --------------------------------------------------------------------------------------------

#: ``(metric name, kind, frozen attribute items, value or None)`` of one recorded data point.
_Point = Tuple[str, str, Tuple[Tuple[str, str], ...], Any]


def _collect(reader: Any) -> List[Dict[str, Any]]:
    """Every data point the reader holds: ``{name, unit, kind, attributes, value}``."""
    found: List[Dict[str, Any]] = []
    data = reader.get_metrics_data()
    if data is None:
        return found
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                kind = "counter" if hasattr(metric.data, "is_monotonic") else "histogram"
                for point in metric.data.data_points:
                    found.append(
                        {
                            "name": metric.name,
                            "unit": metric.unit,
                            "kind": kind,
                            "attributes": dict(point.attributes or {}),
                            "value": point.value if kind == "counter" else point.count,
                            "sum": getattr(point, "sum", None),
                            "bounds": list(getattr(point, "explicit_bounds", []) or []),
                        }
                    )
    return found


def _drive(case: Mapping[str, Any], meter: Optional[Any]) -> Mapping[str, Any]:
    """Run the scenario of a ``consume`` / ``publish`` / ``dead_letter`` case and return the
    envelope the producer handed to the transport (empty for the other modes)."""
    from babelqueue import otel

    mode = case["mode"]
    tracer, _ = _tracing()
    if mode == "consume":
        meta: Dict[str, Any] = {"id": "m-1", "schema_version": 1}
        if "queue" in case:
            meta["queue"] = case["queue"]
        envelope = {"job": "urn:acme:order.created", "trace_id": "t-1", "data": {}, "meta": meta, "attempts": case["attempts"]}

        def handler(data: Any, meta: Any, envelope: Any) -> None:
            if case["outcome"] == "error":
                raise RuntimeError("the handler failed")

        wrapped = otel.wrap_handler(tracer, handler, system=case.get("system"), meter=meter)
        try:
            wrapped({}, meta, envelope)
        except RuntimeError:
            _ensure(case["outcome"] == "error", "unexpected handler failure")
    elif mode == "publish":
        transport = _RecordingTransport(fail=case["outcome"] == "error")
        app = BabelQueue(transport=transport)
        try:
            otel.publish(
                tracer, app, "urn:acme:order.created", {"n": 1},
                queue=case.get("queue"), system=case.get("system"), meter=meter,
            )
        except RuntimeError:
            _ensure(case["outcome"] == "error", "unexpected publish failure")
        if case["outcome"] != "error" or transport.handed_over:
            _ensure(len(transport.handed_over) == 1, "exactly one envelope must be handed to the transport")
        return json.loads(transport.handed_over[0]) if transport.handed_over else {}
    else:  # dead_letter
        observer = otel.dead_letter_observer(meter, system=case.get("system"))
        if case["observer_called"]:
            observer(DeadLetterEvent(**case["event"]))
    return {}


def _expected_records(case: Mapping[str, Any], produced: Mapping[str, Any]) -> List[Dict[str, Any]]:
    records = []
    for record in case["expect"]["records"]:
        attributes = {k: _substitute(v, produced) for k, v in record["attributes"].items()}
        # A destination that resolves to an empty or blank queue is not written at all.
        destination = attributes.get("messaging.destination.name", "x")
        if not destination or not str(destination).strip(_ASCII_WHITESPACE):
            attributes.pop("messaging.destination.name", None)
        records.append({**record, "attributes": attributes})
    return records


def _eval_metrics(case: Mapping[str, Any]) -> None:
    mode = case["mode"]
    if mode == "instrument":
        _eval_instrument(case["expect"])
        return
    meter, reader = _metering()
    produced = _drive(case, None if case.get("meter") is False else meter)
    expected = _expected_records(case, produced)
    points = _collect(reader)

    unmatched = list(points)
    for record in expected:
        want = {k: v for k, v in record["attributes"].items()}
        match = None
        for point in unmatched:
            attributes = dict(point["attributes"])
            error_type = attributes.pop("error.type", None)
            if point["name"] != record["metric"] or attributes != want:
                continue
            if record.get("error_type") == "present":
                if not (isinstance(error_type, str) and error_type):
                    continue
            elif error_type is not None:
                continue
            match = point
            break
        _ensure(match is not None, f"no data point for {record['metric']} {want} (have {points})")
        unmatched.remove(match)
        if record["kind"] == "add":
            _ensure(match["kind"] == "counter" and match["value"] == record["value"], f"counter value {match['value']}")
        else:
            _ensure(match["kind"] == "histogram" and match["value"] == 1, "one histogram observation")
            _ensure(record["value"] == "non_negative_seconds", "unknown histogram value rule")
            _ensure(math.isfinite(match["sum"]) and match["sum"] >= 0, f"duration {match['sum']}")
    _ensure(not unmatched, f"unexpected data points: {unmatched}")
    if case.get("meter") is False or not expected:
        _ensure(not points, f"nothing may be recorded, got {points}")


def _eval_instrument(expect: Mapping[str, Any]) -> None:
    """The metric exists with the pinned instrument kind, unit and exact attribute key set."""
    metric = expect["metric"]
    meter, reader = _metering()
    # One delivery per outcome with a destination, so every conditional key is exercised.
    for outcome in ("ok", "error"):
        for attempts in (0, 1):
            _drive({"mode": "consume", "queue": "orders", "attempts": attempts, "outcome": outcome, "system": "kafka"}, meter)
    for outcome in ("ok", "error"):
        _drive({"mode": "publish", "queue": "orders", "outcome": outcome, "system": "kafka"}, meter)
    _drive({"mode": "dead_letter", "observer_called": True, "system": "kafka", "event": {"queue": "orders", "dead_letter_queue": "orders.dlq", "reason": "failed"}}, meter)

    points = [p for p in _collect(reader) if p["name"] == metric]
    _ensure(points, f"metric {metric} was never recorded")
    always = set(expect["attributes"]["always"])
    conditional = set(expect["attributes"].get("when_destination_known", []))
    on_failure = set(expect["attributes"].get("on_failure", []))
    forbidden = set(expect["forbidden_attributes"])
    saw_failure = False
    for point in points:
        _ensure(point["kind"] == expect["instrument"], f"{metric} is a {point['kind']}")
        _ensure(point["unit"] == expect["unit"], f"{metric} unit {point['unit']!r}")
        keys = set(point["attributes"])
        _ensure(always <= keys, f"{metric}: missing {always - keys}")
        _ensure(conditional <= keys, f"{metric}: destination is known, missing {conditional - keys}")
        _ensure(keys <= always | conditional | on_failure, f"{metric}: extra attributes {keys - (always | conditional | on_failure)}")
        _ensure(not (keys & forbidden), f"{metric}: forbidden attributes {keys & forbidden}")
        _ensure(point["attributes"]["messaging.operation.name"] == expect["operation_name"], "operation name")
        saw_failure = saw_failure or bool(keys & on_failure)
    _ensure(saw_failure == bool(on_failure), f"{metric}: error.type must appear on failure data points only")
    advice = expect.get("advisory_bucket_boundaries")
    if advice is not None:
        # A SHOULD, applied where the installed OpenTelemetry API supports instrument advice.
        import inspect

        from opentelemetry.metrics import Meter

        if "explicit_bucket_boundaries_advisory" in inspect.signature(Meter.create_histogram).parameters:
            _ensure([float(b) for b in points[0]["bounds"]] == [float(b) for b in advice], f"bucket bounds {points[0]['bounds']}")


@unittest.skipUnless(_HAS_OTEL_SDK, "the otel conformance sections need opentelemetry-api and opentelemetry-sdk")
class SpanShapeConformanceTest(unittest.TestCase):
    def test_span_shape_cases(self) -> None:
        _run_cases(self, "span-shape", _eval_span_shape)


@unittest.skipUnless(_HAS_OTEL_SDK, "the otel conformance sections need opentelemetry-api and opentelemetry-sdk")
class TraceIdConformanceTest(unittest.TestCase):
    def test_trace_id_cases(self) -> None:
        _run_cases(self, "trace-id", _eval_trace_id)


@unittest.skipUnless(_HAS_OTEL_SDK, "the otel conformance sections need opentelemetry-api and opentelemetry-sdk")
class MetricsConformanceTest(unittest.TestCase):
    def test_metrics_cases(self) -> None:
        _run_cases(self, "metrics", _eval_metrics)


if __name__ == "__main__":
    unittest.main()
