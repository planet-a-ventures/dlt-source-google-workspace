"""A source loading entities from Google Workspace"""

from enum import StrEnum
from typing import Any, Dict, Iterable, List, Sequence
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from .api_client import get_directory_service


class Table(StrEnum):
    USERS = "users"


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


@dlt.source(name="google_workspace")
def source(domain: str, limit=-1) -> Sequence[DltResource]:
    my_users = users(domain=domain)
    if limit > 0:
        my_users = my_users.add_limit(limit)

    return my_users | user_details()


__all__ = ["source"]
