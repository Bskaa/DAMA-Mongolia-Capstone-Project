"""Build the tap's SQLAlchemy URL safely, then execute the Meltano CLI."""

from __future__ import annotations

import os
import sys
from urllib.parse import quote


def source_database_url() -> str:
    user = quote(os.environ["TAP_POSTGRES_USER"], safe="")
    password = quote(os.environ["TAP_POSTGRES_PASSWORD"], safe="")
    database = quote(os.environ["TAP_POSTGRES_DATABASE"], safe="")
    host = os.environ["TAP_POSTGRES_HOST"]
    port = os.environ["TAP_POSTGRES_PORT"]
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


if __name__ == "__main__":
    os.environ["TAP_POSTGRES_SQLALCHEMY_URL"] = source_database_url()
    os.execvp("meltano", ["meltano", *sys.argv[1:]])
