"""A source loading entities from Google Workspace"""

from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Dict, Iterable, List, Sequence
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from .api_client import get_calendar_service, get_directory_service


class Table(StrEnum):
    USERS = "users"
    CALENDAR_EVENTS = "calendar_events"


# Event types mirrored from the org-calendar-source Pipedream component so the
# two capture the same set of calendar entries.
CALENDAR_EVENT_TYPES = [
    "default",
    "focusTime",
    "outOfOffice",
    "workingLocation",
]

# How far back / forward to poll on every run, in days. Wide and overlapping on
# purpose: each run re-upserts (merge on composite id) whatever falls in the
# window, so a generous window is what lets reschedules/cancellations of
# already-captured events get reflected without needing true incremental cursor
# tracking.
#
# Both are plain function args on the `calendar_events` transformer below, which
# means dlt's config injection can override them per-run WITHOUT a code change or
# a new release — e.g. to backfill the last month once, run with
#   SOURCES__GOOGLE_WORKSPACE__CALENDAR_EVENTS__LOOKBACK_DAYS=31
# set in the environment, then let the normal (small-window) schedule resume.
CALENDAR_LOOKBACK_DAYS = 7
CALENDAR_LOOKAHEAD_DAYS = 90


# TODO: Workaround for the fact that when `add_limit` is used, the yielded entities
# become dicts instead of first-class entities
def __get_id(obj):
    if isinstance(obj, dict):
        return obj.get("id")
    return getattr(obj, "id", None)


def use_id(entity: Dict[str, Any], **kwargs) -> dict:
    return entity | {"_dlt_id": __get_id(entity)}


@dlt.resource(
    selected=False,
    parallelized=True,
    write_disposition="merge",
    merge_key="id",
)
def users(domain: str) -> Iterable[TDataItem]:
    directory_service = get_directory_service()

    next_page_token = None
    while True:
        results = (
            directory_service.users()
            .list(domain=domain, maxResults=500, pageToken=next_page_token)
            .execute()
        )
        yield results.get("users", [])
        next_page_token = results.get("nextPageToken", None)
        if next_page_token is None:
            break


@dlt.transformer(
    max_table_nesting=1,
    parallelized=True,
)
async def user_details(users: List[Any]):
    for user in users:
        yield dlt.mark.with_hints(
            item=use_id({key: user[key] for key in user if key not in ["kind"]}),
            hints=dlt.mark.make_hints(
                table_name=Table.USERS.value,
                primary_key="id",
                merge_key="id",
                write_disposition="merge",
            ),
            # needs to be a variant due to https://github.com/dlt-hub/dlt/pull/2109
            create_table_variant=True,
        )


def use_composite_id(entity: Dict[str, Any], owner_id: str) -> dict:
    # Event ids are only unique *within* a single calendar — the same meeting
    # shows up with the same event id on every attendee's calendar — so the
    # dedupe/merge key has to be scoped per calendar owner, same as
    # org-calendar-source's `${entry.id}__${user.id}` emit id.
    return entity | {"_dlt_id": f"{entity.get('id')}__{owner_id}"}


@dlt.transformer(
    max_table_nesting=1,
    parallelized=True,
)
async def calendar_events(
    users: List[Any],
    event_types: Sequence[str] = CALENDAR_EVENT_TYPES,
    lookback_days: int = CALENDAR_LOOKBACK_DAYS,
    lookahead_days: int = CALENDAR_LOOKAHEAD_DAYS,
):
    # `lookback_days` / `lookahead_days` are dlt-injectable: override via config
    # or env (SOURCES__GOOGLE_WORKSPACE__CALENDAR_EVENTS__LOOKBACK_DAYS=...) for
    # one-off backfills without touching code.
    now = datetime.now(timezone.utc)
    time_min = now - timedelta(days=lookback_days)
    time_max = now + timedelta(days=lookahead_days)

    for user in users:
        primary_email = user.get("primaryEmail")
        org_unit_path = user.get("orgUnitPath")
        owner_id = user.get("id") or primary_email

        if not primary_email or org_unit_path != "/":
            continue

        calendar_service = get_calendar_service(primary_email)

        page_token = None
        while True:
            results = (
                calendar_service.events()
                .list(
                    calendarId="primary",
                    singleEvents=True,
                    orderBy="startTime",
                    eventTypes=list(event_types),
                    timeMin=time_min.isoformat(),
                    timeMax=time_max.isoformat(),
                    maxResults=2500,
                    pageToken=page_token,
                )
                .execute()
            )

            for entry in results.get("items", []):
                yield dlt.mark.with_hints(
                    item=use_composite_id(entry, owner_id)
                    | {
                        "calendar_owner_id": user.get("id"),
                        "calendar_owner_email": primary_email,
                    },
                    hints=dlt.mark.make_hints(
                        table_name=Table.CALENDAR_EVENTS.value,
                        primary_key="_dlt_id",
                        merge_key="_dlt_id",
                        write_disposition="merge",
                    ),
                    # needs to be a variant due to https://github.com/dlt-hub/dlt/pull/2109
                    create_table_variant=True,
                )

            page_token = results.get("nextPageToken")
            if not page_token:
                break


@dlt.source(name="google_workspace")
def source(domain: str, limit=-1) -> Sequence[DltResource]:
    my_users = users(domain=domain)
    if limit > 0:
        my_users = my_users.add_limit(limit)

    return (my_users | user_details()), (my_users | calendar_events())


__all__ = ["source"]
