from typing import Any, Dict
import dlt
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
]

CALENDAR_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
]

# TODO: Replace Any with the correct type
directory_service: Any | None = None

# One delegated Calendar API client per impersonated user, keyed by email.
_calendar_services: Dict[str, Any] = {}


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


def get_calendar_service(
    delegated_user_email: str,
    service_account_info: str = dlt.secrets["google_workspace_service_account_info"],
):
    """Build (and cache) a Calendar API client impersonating a single user via
    domain-wide delegation. Unlike the Directory API, no admin impersonation is
    needed here — the service account can impersonate any user directly for
    scopes granted to it in the Admin Console."""
    if delegated_user_email in _calendar_services:
        return _calendar_services[delegated_user_email]

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=CALENDAR_SCOPES
    )
    delegated_credentials = credentials.with_subject(delegated_user_email)

    calendar_service = build("calendar", "v3", credentials=delegated_credentials)
    _calendar_services[delegated_user_email] = calendar_service
    return calendar_service
