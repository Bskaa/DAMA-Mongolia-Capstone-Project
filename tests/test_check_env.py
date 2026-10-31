from __future__ import annotations

import unittest

from scripts.check_env import audit_env

GOOD = {
    "POSTGRES_USER": "gaming",
    "POSTGRES_PASSWORD": "Str0ng-source-pass",
    "POSTGRES_DB": "gaming_source",
    "POSTGRES_PORT": "5432",
    "DATABASE_URL": "postgresql://gaming:Str0ng-source-pass@localhost:5432/gaming_source",
    "ANALYTICS_DB": "gaming_analytics",
    "DASHBOARD_DB_PASSWORD": "another-dashboard-secret",
    "AIRFLOW_DB_PASSWORD": "airflow_db_secret_123",
    "AIRFLOW__API_AUTH__JWT_SECRET": "x" * 40,
    "RAWG_API_KEY": "key",
}


def levels(env):
    return {level for level, _ in audit_env(env)}


class CheckEnvTests(unittest.TestCase):
    def test_a_good_env_has_no_errors(self) -> None:
        self.assertNotIn("ERROR", levels(GOOD))

    def test_example_placeholders_are_errors(self) -> None:
        env = dict(GOOD, POSTGRES_PASSWORD="change-this-local-password",
                   DATABASE_URL="postgresql://gaming:change-this-local-password@localhost:5432/gaming_source")
        messages = [message for level, message in audit_env(env) if level == "ERROR"]
        self.assertTrue(any("placeholder" in message for message in messages))

    def test_database_url_must_match_the_password(self) -> None:
        env = dict(GOOD, DATABASE_URL="postgresql://gaming:other@localhost:5432/gaming_source")
        self.assertIn("DATABASE_URL password does not match POSTGRES_PASSWORD",
                      [message for _, message in audit_env(env)])

    def test_dashboard_must_not_reuse_the_owner_password(self) -> None:
        env = dict(GOOD, DASHBOARD_DB_PASSWORD=GOOD["POSTGRES_PASSWORD"])
        self.assertIn("ERROR", levels(env))

    def test_airflow_db_password_must_be_url_safe(self) -> None:
        self.assertIn("ERROR", levels(dict(GOOD, AIRFLOW_DB_PASSWORD="p@ss/word#123")))

    def test_short_jwt_secret_is_an_error(self) -> None:
        self.assertIn("ERROR", levels(dict(GOOD, AIRFLOW__API_AUTH__JWT_SECRET="short")))

    def test_values_are_never_printed(self) -> None:
        env = dict(GOOD, DASHBOARD_DB_PASSWORD=GOOD["POSTGRES_PASSWORD"])
        self.assertFalse(any(GOOD["POSTGRES_PASSWORD"] in message for _, message in audit_env(env)))


if __name__ == "__main__":
    unittest.main()
