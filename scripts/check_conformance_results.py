#!/usr/bin/env python3
"""Completeness gate for a conformance ``results.json`` (ADR-0032 section 3 / Compliance).

Usage: python scripts/check_conformance_results.py <results.json> <conformance-dir> [src-dir]

Known gaps: a ``fail`` is accepted only when ``conformance-known-gaps.json`` (repo root) lists its
section and case; a listed case that reports ``pass`` is a stale gap and fails the gate, as does
a ``skip`` or a missing result for a listed case, and a gap naming a case the manifest lacks.

Shared by ``ci.yml`` and ``release.yml`` so a tag cannot publish an asset CI would have refused.
It is deliberately independent of the test runner: the carried bindings are re-derived from the
package modules and compared for *set equality* with ``results.bindings`` (a missing binding and
a surplus one both fail), then the sections are checked against the manifest scopes.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import jsonschema

# Transport module -> binding (manifest scope). Keep in step with tests/test_conformance.py.
TRANSPORT_BINDINGS = {
    "redis_transport": "redis",
    "pika_transport": "amqp",
    "sqs_transport": "sqs",
    "asb_transport": "asb",
    "pulsar_transport": "pulsar",
    "kafka_transport": "kafka",
    "artemis_transport": "artemis",
}


def derive_bindings(src: Path) -> set:
    bindings = set()
    for path in sorted(src.glob("*_transport.py")):
        if path.stem not in TRANSPORT_BINDINGS:
            raise SystemExit(f"transport module {path.name} has no binding mapping")
        bindings.add(TRANSPORT_BINDINGS[path.stem])
    otel = src / "otel.py"
    if otel.is_file() and "traceparent" in otel.read_text(encoding="utf-8").lower():
        bindings.add("otel")
    return bindings


GAPS_FILE = Path(__file__).resolve().parent.parent / "conformance-known-gaps.json"

#: A reason must cite the contract section and the follow-up plan, so a gap is never anonymous.
REASON_PATTERN = re.compile(r"section[s]? \d")
REASON_PLAN = "r1 follow-up plan"


def check_gap_entry(entry: dict, manifest: dict) -> list:
    """Problems with one known-gap entry against the manifest (empty when it is well formed)."""
    problems = []
    section, case, reason = entry["section"], entry["case"], entry["reason"]
    definition = manifest["sections"].get(section)
    if definition is None:
        problems.append(f"{section}/{case}: unknown section")
    elif definition["scope"] == "core":
        # Only a broker-scoped feature may be a documented gap; the core envelope never is.
        problems.append(f"{section}/{case}: a core-scope section cannot have a known gap")
    if not REASON_PATTERN.search(reason) or REASON_PLAN not in reason:
        problems.append(f'{section}/{case}: reason must cite a contract "section N" and the "{REASON_PLAN}"')
    return problems


def load_gaps(path: Path, errors: list, manifest: dict) -> dict:
    """``(section, case) -> reason`` from the known-gaps file; malformed entries are errors."""
    if not path.is_file():
        return {}
    document = json.loads(path.read_text(encoding="utf-8"))
    gaps: dict = {}
    entry_problems: list = []
    entries = document.get("gaps") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        errors.append(f'{path.name}: top level must be {{"gaps": [...]}}')
        return gaps
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict) or not all(
            isinstance(entry.get(key), str) and entry[key].strip() for key in ("section", "case", "reason")
        ):
            errors.append(f"{path.name}: gaps[{index}] needs non-empty string section, case and reason")
            continue
        key = (entry["section"], entry["case"])
        entry_problems.extend(check_gap_entry(entry, manifest))
        if key in gaps:
            errors.append(f"{path.name}: duplicate gap {key[0]}/{key[1]}")
        gaps[key] = entry["reason"]
    errors.extend(f"{path.name}: {problem}" for problem in entry_problems)
    return gaps


def main(argv: list) -> int:
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    results_path, suite = Path(argv[0]), Path(argv[1])
    src = Path(argv[2]) if len(argv) == 3 else Path("src/babelqueue")

    results = json.loads(results_path.read_text(encoding="utf-8"))
    manifest = json.loads((suite / "manifest.json").read_text(encoding="utf-8"))
    schema = json.loads((suite / "schema" / "results.schema.json").read_text(encoding="utf-8"))

    errors = []
    try:
        jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(results)
    except jsonschema.ValidationError as exc:
        errors.append(f"results.json does not match results.schema.json: {exc.message}")

    carried = derive_bindings(src)
    listed = set(results.get("bindings", []))
    for name in sorted(carried - listed):
        errors.append(f'binding "{name}" is carried by the package but missing from results.bindings')
    for name in sorted(listed - carried):
        errors.append(f'results.bindings lists "{name}", which the package does not carry')

    sections = results.get("sections", {})
    for name, definition in manifest["sections"].items():
        in_scope = definition["scope"] == "core" or definition["scope"] in listed
        if in_scope and name not in sections:
            errors.append(f'in-scope section "{name}" (scope {definition["scope"]}) is missing')
        if not in_scope and name in sections:
            errors.append(f'section "{name}" (scope {definition["scope"]}) must be absent')

    gaps = load_gaps(GAPS_FILE, errors, manifest)
    for (section, case_id), _reason in sorted(gaps.items()):
        outcome = sections.get(section, {}).get(case_id)
        if outcome is None:
            errors.append(f"{section}/{case_id}: listed as a known gap but absent from results.json")
        elif outcome.get("status") == "pass":
            errors.append(f"{section}/{case_id}: stale gap (listed in {GAPS_FILE.name} but passes); remove it")
        elif outcome.get("status") == "skip":
            errors.append(f"{section}/{case_id}: a known gap must be reported as fail, not skip")

    failed = []
    for section, cases in sections.items():
        for case_id, outcome in cases.items():
            if outcome.get("status") == "fail" and (section, case_id) not in gaps:
                failed.append(f"{section}/{case_id}")
    errors.extend(f"{case}: fail (not a known gap)" for case in failed)

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    known = sum(
        1
        for section, cases in sections.items()
        for case_id, outcome in cases.items()
        if outcome.get("status") == "fail" and (section, case_id) in gaps
    )
    print(f"results.json complete: bindings={sorted(listed)}, sections={len(sections)}, fail={known} (all known gaps)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
