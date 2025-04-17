from typing import Any
import dlt
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
]

# TODO: Replace Any with the correct type
directory_service: Any | None = None


def get_directory_service(
    service_account_info: str = dlt.secrets["google_workspace_service_account_info"],
    admin_user_email: str = dlt.config.get("admin_user_email"),
):
    global directory_service

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES
    )
    delegated_credentials = credentials.with_subject(admin_user_email)

    if directory_service is None:
        # Authenticate with Directory API (to list all users)
        directory_service = build(
            "admin", "directory_v1", credentials=delegated_credentials
        )
    return directory_service
