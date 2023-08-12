from __future__ import annotations
import os
from typing import (
    Any,
    Callable,
    Literal,
    Type,
    TypedDict,
    Union,
    TYPE_CHECKING,
)
from typing_extensions import TypeAlias, NotRequired

from tenacity import stop_after_attempt, wait_random_exponential

if TYPE_CHECKING:
    from tenacity import (
        StopBaseT,
        WaitBaseT,
        RetryBaseT,
        RetryCallState,
        RetryError,
    )

HASURA_URL = os.environ["HASURA_URL"]
HASURA_ADMIN_SECRET = os.environ["HASURA_ADMIN_SECRET"]
HASURA_ROLE = os.environ["HASURA_ROLE"]


class HasuraConfig(TypedDict):
    url: str
    headers: dict[str, str]


class RetryConfig(TypedDict):
    sleep: NotRequired[Callable[[Union[int, float]], None]]
    stop: NotRequired[StopBaseT]
    wait: NotRequired[WaitBaseT]
    retry: NotRequired[RetryBaseT]
    before: NotRequired[Callable[[RetryCallState], None]]
    after: NotRequired[Callable[[RetryCallState], None]]
    before_sleep: NotRequired[Callable[[RetryCallState], None]]
    reraise: NotRequired[bool]
    retry_error_cls: NotRequired[Type[RetryError]]
    retry_error_callback: NotRequired[Callable[[RetryCallState], Any]]


class CuckooConfig(HasuraConfig):
    retry: RetryConfig


HASURA_HEADERS = {
    "X-Hasura-Admin-Secret": HASURA_ADMIN_SECRET,
    "X-Hasura-Role": HASURA_ROLE,
}

HASURA_DEFAULT_CONFIG: HasuraConfig = {
    "url": HASURA_URL,
    "headers": HASURA_HEADERS,
}

RETRY_DEFAULT_CONFIG: RetryConfig = {
    "wait": wait_random_exponential(multiplier=1, max=60),
    "stop": stop_after_attempt(5),
}

WHERE: TypeAlias = dict[str, Any]
ORDER_DIRECTION: TypeAlias = Union[Literal["asc"], Literal["desc"]]
ORDER_BY: TypeAlias = dict[str, Union[ORDER_DIRECTION, "ORDER_BY"]]
