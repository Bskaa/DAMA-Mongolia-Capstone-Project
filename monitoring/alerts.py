"""Turn monitoring results into alerts: one email, one alert_log row per problem.

Two ways in, one way out:

    python -m monitoring.alerts          run checks.sql; every row it returns is an alert
    task_failed(...)                     called by the Airflow failure callback the moment a
                                         task goes red (via monitoring.task_events --alert)

    -> one email to QUALITY_ALERT_EMAIL_TO through the AIRFLOW__SMTP__* settings in .env
    -> one row per alert in monitoring.alert_log (what fired, when, whether it was sent)

Rules for alerts that people keep reading:
  1. An alert nobody receives is a log line   -> it is emailed, and logged either way.
  2. The same alert twice is noise            -> each (check, object) is sent once per day.
  3. "All clear" is a message too             -> the daily run sends a short ✅ when nothing
                                                 fired; no email at all means the pipeline is dead.

    python -m monitoring.alerts --dry-run         show what would be sent; send and log nothing
    python -m monitoring.alerts --quiet-if-ok     no ✅ message (used by the watchdog DAG)

Exit code 1 when a critical problem exists, so the Airflow task turns red: "the data is
wrong" finally looks different from "the data is fine" in the Airflow grid.
"""

from __future__ import annotations

import argparse
import os
import smtplib
import socket
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Iterable, Sequence

from monitoring.ops_store import connect, ensure_ops_tables

CHECKS_SQL = Path(__file__).with_name("checks.sql")
ICON = {"critical": "🔴", "warn": "🟡"}
MAX_EXAMPLES_PER_CHECK = 5

Alert = tuple[str, str, str, str]  # severity, check_name, object, detail


def pipeline_name() -> str:
    return os.environ.get("ALERT_NAME") or os.environ.get("AIRFLOW__API__INSTANCE_NAME") or socket.gethostname()


# --- pure helpers (unit-tested) -------------------------------------------------


def sort_alerts(alerts: Iterable[Alert]) -> list[Alert]:
    return sorted(alerts, key=lambda alert: (alert[0] != "critical", alert[1], alert[2]))


def new_alerts(alerts: Sequence[Alert], already_sent: set[tuple[str, str]]) -> list[Alert]:
    """Drop problems already emailed today (rule 2)."""
    return [alert for alert in alerts if (alert[1], alert[2]) not in already_sent]


def compose_email(new: Sequence[Alert], repeated: int, who: str) -> tuple[str, str]:
    """One block per rule with at most five examples, so one noisy rule cannot bury another."""
    body = ""
    checks = list(dict.fromkeys(alert[1] for alert in new))
    for check in checks:
        group = [alert for alert in new if alert[1] == check]
        count = f" x{len(group)}" if len(group) > 1 else ""
        body += f"\n{ICON[group[0][0]]} {check}{count}\n"
        for _, _, obj, detail in group[:MAX_EXAMPLES_PER_CHECK]:
            body += f"   {obj}: {detail}\n"
        if len(group) > MAX_EXAMPLES_PER_CHECK:
            body += f"   ... {len(group) - MAX_EXAMPLES_PER_CHECK} more in monitoring.alert_log\n"
    if repeated:
        body += f"\n(+{repeated} already reported today)\n"
    worst = "🔴" if any(alert[0] == "critical" for alert in new) else "🟡"
    subject = f"{worst} {who}: {len(new)} new problem(s) · " + ", ".join(checks)
    return subject, body


# --- email ----------------------------------------------------------------------


def smtp_settings() -> dict[str, Any] | None:
    host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "").strip()
    recipients = [
        address.strip()
        for address in os.environ.get("QUALITY_ALERT_EMAIL_TO", "").split(",")
        if address.strip()
    ]
    enabled = os.environ.get("QUALITY_EMAIL_ENABLED", "true").strip().lower() not in ("0", "false", "no")
    if not (enabled and host and recipients):
        return None
    return {
        "host": host,
        "port": int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "587") or 587),
        "user": os.environ.get("AIRFLOW__SMTP__SMTP_USER", "").strip(),
        "password": os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD", ""),
        "sender": os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", "").strip()
        or os.environ.get("AIRFLOW__SMTP__SMTP_USER", "").strip(),
        "starttls": os.environ.get("AIRFLOW__SMTP__SMTP_STARTTLS", "true").lower() == "true",
        "ssl": os.environ.get("AIRFLOW__SMTP__SMTP_SSL", "false").lower() == "true",
        "to": recipients,
    }


def send_email(subject: str, body: str) -> bool:
    """True if the mail server accepted it. Never raises: a broken alert must not break the pipeline."""
    settings = smtp_settings()
    if settings is None:
        print("  (SMTP / QUALITY_ALERT_EMAIL_TO not configured: logged, not sent)")
        return False
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings["sender"] or settings["to"][0]
    message["To"] = ", ".join(settings["to"])
    message.set_content(body)
    try:
        if settings["ssl"]:
            server: smtplib.SMTP = smtplib.SMTP_SSL(
                settings["host"], settings["port"], timeout=20, context=ssl.create_default_context()
            )
        else:
            server = smtplib.SMTP(settings["host"], settings["port"], timeout=20)
        with server:
            if settings["starttls"] and not settings["ssl"]:
                server.starttls(context=ssl.create_default_context())
            if settings["user"]:
                try:
                    server.login(settings["user"], settings["password"])
                except (smtplib.SMTPAuthenticationError, smtplib.SMTPServerDisconnected):
                    print("  email not sent: login refused. Gmail needs a 16-character App "
                          "Password (2-Step Verification on), not the account password.")
                    return False
            server.send_message(message)
        return True
    except Exception as error:  # noqa: BLE001 - alerting must never raise
        print(f"  email not sent: {error}")
        return False


# --- database -------------------------------------------------------------------


def log_alerts(cursor: Any, alerts: Sequence[Alert], source: str, sent: bool) -> None:
    cursor.executemany(
        """
        INSERT INTO monitoring.alert_log (alert_date, source, severity, check_name, object, detail, sent)
        VALUES (current_date, %s, %s, %s, %s, %s, %s)
        """,
        [(source, severity, check, obj, detail, sent) for severity, check, obj, detail in alerts],
    )


def sent_today(cursor: Any) -> set[tuple[str, str]]:
    cursor.execute(
        "SELECT DISTINCT check_name, object FROM monitoring.alert_log "
        "WHERE alert_date = current_date AND sent"
    )
    return {(check, obj) for check, obj in cursor.fetchall()}


def evaluate(cursor: Any) -> list[Alert]:
    cursor.execute(CHECKS_SQL.read_text(encoding="utf-8"))
    return sort_alerts(tuple(str(value) for value in row) for row in cursor.fetchall())


def run_checks(dry_run: bool = False, quiet_if_ok: bool = False, source: str = "checks") -> bool:
    """Evaluate the rules, email and log new problems. Returns True if anything is critical."""
    with connect() as connection, connection.cursor() as cursor:
        ensure_ops_tables(cursor)
        alerts = evaluate(cursor)
        for severity, check, obj, detail in alerts:
            print(f"  {severity:<8} {check:<17} {obj:<55} {detail}")
        if not alerts:
            print("  all checks passed")
        critical = any(alert[0] == "critical" for alert in alerts)
        if dry_run:
            print("dry run: nothing sent, nothing logged")
            return critical

        new = new_alerts(alerts, sent_today(cursor))
        if new:
            subject, body = compose_email(new, len(alerts) - len(new), pipeline_name())
            log_alerts(cursor, new, source, send_email(subject, body))
        elif not alerts and not quiet_if_ok:
            send_email(f"✅ {pipeline_name()}: all checks passed", "All monitoring rules passed, 0 problems.")
        print(f"{len(alerts)} problem(s), {len(new)} new")
        return critical


def task_failed(dag_id: str, task_id: str, run_id: str, try_number: int) -> None:
    """The pipeline itself broke: alert immediately (called from the Airflow failure callback)."""
    if task_id in ("check_and_alert", "watchdog"):
        return  # these already sent their own message before going red
    alert: Alert = ("critical", "task_failed", f"{dag_id} . {task_id}", f"try {try_number} . run {run_id}")
    sent = send_email(
        f"🔴 {pipeline_name()}: task failed · {dag_id} · {task_id}",
        f"{alert[2]}\n{alert[3]}\n\nOpen the task log in Airflow and read the FIRST error, not the last.",
    )
    try:
        with connect() as connection, connection.cursor() as cursor:
            ensure_ops_tables(cursor)
            log_alerts(cursor, [alert], "airflow", sent)
    except Exception as error:  # noqa: BLE001 - a database outage must not hide the email
        print(f"  could not log the alert: {error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="show, do not send or log")
    parser.add_argument("--quiet-if-ok", action="store_true", help="no ✅ message (watchdog)")
    parser.add_argument("--source", default="checks", help="label stored in alert_log.source")
    args = parser.parse_args(argv)
    return 1 if run_checks(args.dry_run, args.quiet_if_ok, args.source) else 0


if __name__ == "__main__":
    sys.exit(main())
