"""
Shared utilities for Databricks A/B Testing framework
"""

from typing import Optional

from databricks.sdk import WorkspaceClient


def get_app_service_principal_id(client: WorkspaceClient, app_name: str) -> Optional[str]:
    """
    Get the application_id (UUID) for the service principal associated with a Databricks App.

    Args:
        client: WorkspaceClient instance
        app_name: Name of the Databricks App

    Returns:
        Service principal application_id (UUID) or None if not found
    """
    try:
        app = client.apps.get(name=app_name)
        sp = client.service_principals.get(id=app.service_principal_id)
        return sp.application_id
    except Exception as e:
        print(f"Warning: Could not get app service principal: {e}")
        return None
