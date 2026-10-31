from __future__ import annotations

import unittest
from pathlib import Path
from urllib.parse import urlparse

from scripts.check_env import audit_env
from scripts.generate_env import render

EXAMPLE = (Path(__file__).resolve().parents[1] / ".env.example").read_text(encoding="utf-8")


def parse(text: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#"))


class GenerateEnvTests(unittest.TestCase):
    def test_generated_env_passes_the_audit(self) -> None:
        env = parse(render(EXAMPLE))
        self.assertEqual([m for level, m in audit_env(env) if level == "ERROR"], [])

    def test_database_url_carries_the_new_password(self) -> None:
        env = parse(render(EXAMPLE))
        self.assertEqual(urlparse(env["DATABASE_URL"]).password, env["POSTGRES_PASSWORD"])

    def test_every_run_creates_different_secrets(self) -> None:
        self.assertNotEqual(parse(render(EXAMPLE))["POSTGRES_PASSWORD"], parse(render(EXAMPLE))["POSTGRES_PASSWORD"])


if __name__ == "__main__":
    unittest.main()
