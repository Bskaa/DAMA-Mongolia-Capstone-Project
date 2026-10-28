import os
import time
from urllib.error import URLError
from urllib.request import urlopen

from metadata.workflow.metadata import MetadataWorkflow


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set before running metadata ingestion")
    return value


def wait_for_server(url: str) -> None:
    health_url = url.removesuffix("/api") + "/healthcheck"
    for attempt in range(30):
        try:
            with urlopen(health_url, timeout=5):
                return
        except (OSError, URLError):
            if attempt == 29:
                raise RuntimeError(
                    f"OpenMetadata server did not become healthy at {health_url}"
                )
            time.sleep(5)


def main() -> None:
    server_url = required_env("OPENMETADATA_HOST_PORT")
    jwt_token = required_env("OPENMETADATA_JWT_TOKEN")
    database_user = required_env("POSTGRES_USER")
    database_password = required_env("POSTGRES_PASSWORD")
    database_name = required_env("ANALYTICS_DB")
    service_name = required_env("OPENMETADATA_SERVICE_NAME")

    wait_for_server(server_url)

    workflow = MetadataWorkflow.create(
        {
            "source": {
                "type": "postgres",
                "serviceName": service_name,
                "serviceConnection": {
                    "config": {
                        "type": "Postgres",
                        "username": database_user,
                        "authType": {"password": database_password},
                        "hostPort": "postgres-analytics:5432",
                        "database": database_name,
                    }
                },
                "sourceConfig": {
                    "config": {
                        "type": "DatabaseMetadata",
                        "includeTables": True,
                        "includeViews": True,
                        "schemaFilterPattern": {
                            "includes": ["^(raw|staging|staging_core)$"]
                        },
                    }
                },
            },
            "sink": {"type": "metadata-rest", "config": {}},
            "workflowConfig": {
                "openMetadataServerConfig": {
                    "hostPort": server_url,
                    "authProvider": "openmetadata",
                    "securityConfig": {"jwtToken": jwt_token},
                }
            },
        }
    )
    try:
        workflow.execute()
        workflow.raise_from_status()
        workflow.print_status()
    finally:
        workflow.stop()


if __name__ == "__main__":
    main()
