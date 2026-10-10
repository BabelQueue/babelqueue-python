"""Broker-free guard for the live-Kafka tests in ``test_kafka_integration.py``.

The live tests only run against a real broker (CI push / manual), so a mistake in their own logic
(wrong handler signature, an un-acked read that is then redelivered, ...) would stay invisible until
that job runs. This test executes the very same test module in a child interpreter with
``BQ_INTEGRATION=1`` against ``tests/fakes/confluent_kafka`` -- a tiny in-process stand-in that models
the semantics those tests rely on (single partition, committed offsets, ``seek``, redelivery of
un-committed records). It proves the test logic, not the broker.
"""

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FAKES = ROOT / "tests" / "fakes"


class KafkaIntegrationLogicTest(unittest.TestCase):
    def test_live_kafka_tests_pass_against_in_process_fake(self) -> None:
        env = dict(os.environ)
        env["BQ_INTEGRATION"] = "1"
        env["PYTHONPATH"] = os.pathsep.join(
            [str(FAKES), str(ROOT / "src"), env.get("PYTHONPATH", "")]
        )
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
             str(ROOT / "tests" / "test_kafka_integration.py")],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("2 passed", proc.stdout, proc.stdout)


if __name__ == "__main__":
    unittest.main()
