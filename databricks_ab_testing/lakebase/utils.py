"""
Shared utilities for Lakebase database operations
"""

import uuid
from contextlib import contextmanager
from typing import Generator

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.database import (
    DatabaseInstanceRole,
    DatabaseInstanceRoleIdentityType,
)
from psycopg import Cursor


@contextmanager
def pg_cursor(client: WorkspaceClient, instance_name: str, database_name: str) -> Generator[Cursor, None, None]:
    """
    Context manager for getting a Postgres cursor to Lakebase.

    Automatically handles:
    - Credential generation
    - Connection setup
    - Cleanup

    Args:
        client: WorkspaceClient instance
        instance_name: Lakebase instance name
        database_name: Logical database name

    Yields:
        Cursor for executing SQL
    """
    credentials = client.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[instance_name]
    )

    instance = client.database.get_database_instance(name=instance_name)
    user = client.current_user.me().user_name

    with psycopg.connect(
        host=instance.read_write_dns,
        dbname=database_name,
        user=user,
        password=credentials.token,
        sslmode="require",
        autocommit=True,
    ) as connection:
        with connection.cursor() as cur:
            yield cur


def ensure_database_instance_role(client: WorkspaceClient, instance_name: str, service_principal_name: str) -> None:
    """
    Ensure a service principal has a database instance role.

    Args:
        client: WorkspaceClient instance
        instance_name: Lakebase instance name
        service_principal_name: Service principal application_id (UUID)
    """
    try:
        client.database.get_database_instance_role(instance_name, service_principal_name)
        print(f"  Role {service_principal_name} already exists")
    except NotFound:
        print(f"  Creating role for {service_principal_name}")
        role = DatabaseInstanceRole(
            identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL,
            name=service_principal_name,
        )
        client.database.create_database_instance_role(instance_name, role)


def grant_schema_and_table_permissions(
    cursor: Cursor,
    schema: str,
    table: str,
    grantee: str,
    permissions: list[str] = None,
) -> None:
    """
    Grant permissions on a schema and table to a user/service principal.

    Args:
        cursor: Postgres cursor
        schema: Schema name
        table: Table name
        grantee: User or service principal to grant to
        permissions: List of permissions (default: SELECT)
    """
    if permissions is None:
        permissions = ["SELECT"]

    perms_str = ", ".join(permissions)

    grants = [
        f'GRANT USAGE ON SCHEMA {schema} TO "{grantee}";',
        f'GRANT {perms_str} ON {schema}.{table} TO "{grantee}";',
    ]

    for grant in grants:
        cursor.execute(grant)
