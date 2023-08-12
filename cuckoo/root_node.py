from __future__ import annotations
from logging import Logger
from types import GeneratorType
from typing import (
    Any,
    Optional,
    Union,
)

from httpx import Client, Response, AsyncClient
import orjson
from pydantic import BaseModel
from tenacity import AsyncRetrying, Retrying, RetryError

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import HASURA_DEFAULT_CONFIG, RETRY_DEFAULT_CONFIG, CuckooConfig
from cuckoo.encoders import jsonable_encoder
from cuckoo.errors import HasuraClientError, HasuraServerError
from cuckoo.models import TMODEL
from cuckoo.utils import in_brackets, to_truncated_str


class RootNode(BinaryTreeNode[TMODEL]):
    VAR_NAME_BASE = "var"

    def __init__(
        self: RootNode,
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._config: CuckooConfig = {
            **HASURA_DEFAULT_CONFIG,  # type: ignore
            "retry": RETRY_DEFAULT_CONFIG,
            **(config if config else {}),
        }
        self._session = session
        self._session_async = session_async
        self._logger = logger
        self._var_name_counter: int = 0
        self._response_data: Optional[dict[str, Any]] = None
        self._is_batch = False
        self._close_session = False

    def __str__(self: RootNode):
        outer_args = self._get_all_outer_args()
        return f"""
            {self._fragments.query_name}{
                in_brackets(', '.join(outer_args), condition=bool(outer_args))
            } {{
                {" ".join(str(child) for child in self._children)}
            }}
        """

    def _get_response(
        self: RootNode,
        query_alias: str,
        key: Optional[str] = None,
    ) -> Union[dict, list, int]:
        if self._response_data is None:
            raise HasuraClientError(
                "Cannot access response data before calling `RootNode._make_request`"
            )

        if key:
            return (
                self._response_data[query_alias].pop(key, None)
                if (
                    query_alias in self._response_data
                    and isinstance(self._response_data[query_alias], dict)
                )
                else None
            )
        else:
            return self._response_data.pop(query_alias, None)

    def _generate_var_name(self: RootNode):
        self._var_name_counter += 1
        return f"{self.VAR_NAME_BASE}{self._var_name_counter}"

    def _compact(self: RootNode, gql_string: str):
        return " ".join(gql_string.split())

    def _get_or_create_session(self, use_async=False):
        if use_async:
            if self._session_async is None:
                self._close_session = True
                self._session_async = AsyncClient(timeout=None)
            return self._session_async
        else:
            if self._session is None:
                self._close_session = True
                self._session = Client(timeout=None)
            return self._session

    def _process_response(self, response: Response, query: str, variables: dict):
        response.raise_for_status()
        response_json: dict[str, Any] = response.json()

        if ("error" in response_json) or ("errors" in response_json):
            errors: list[dict[str, str]] = (
                [response_json.get("error")] if "error" in response_json else []
            ) + response_json.get("errors", [])
            if self._logger:
                self._logger.error(
                    "Query failed. query=%s, variables=%s, response=%s",
                    self._compact(query),
                    to_truncated_str(variables),
                    str(errors),
                )
            raise HasuraServerError(errors)

        elif "data" in response_json:
            if self._logger:
                self._logger.debug(
                    "Query successful. query=%s, variables=%s, response=%s",
                    self._compact(query),
                    to_truncated_str(variables),
                    to_truncated_str(response_json["data"]),
                )
            return response_json["data"]

        else:
            raise NotImplementedError(
                "response dict did not contain any errors nor data."
            )

    def _execute(self: RootNode):
        query = str(self)
        variables = self._get_all_variables()
        json = {"query": self._compact(query)}
        if variables:
            json["variables"] = RootNode._jsonify_variables(variables)

        if self._logger:
            self._logger.debug(
                "Executing Query. query=%s, variables=%s, config=%s",
                self._compact(query),
                to_truncated_str(variables),
                self._config,
            )

        session = self._get_or_create_session()
        try:
            for attempt in Retrying(**self._config["retry"]):
                with attempt:
                    response = session.post(
                        url=self._config["url"],
                        headers=self._config["headers"],
                        json=json,
                    )
        except RetryError as err:
            raise HasuraServerError(repr(err))
        finally:
            if self._close_session and self._session is not None:
                self._session.close()

        self._response_data = self._process_response(
            response=response, query=query, variables=variables
        )

    async def _execute_async(self):
        query = str(self)
        variables = self._get_all_variables()
        json = {"query": self._compact(query)}
        if variables:
            json["variables"] = RootNode._jsonify_variables(variables)

        if self._logger:
            self._logger.debug(
                "Executing Query. query=%s, variables=%s, config=%s",
                self._compact(query),
                to_truncated_str(variables),
                self._config,
            )

        session = self._get_or_create_session(use_async=True)
        try:
            async for attempt in AsyncRetrying(**self._config["retry"]):
                with attempt:
                    response = await session.post(
                        url=self._config["url"],
                        headers=self._config["headers"],
                        json=json,
                    )
        except RetryError as err:
            raise HasuraServerError(repr(err))
        finally:
            if self._close_session and self._session_async is not None:
                await self._session_async.aclose()

        self._response_data = self._process_response(
            response=response, query=query, variables=variables
        )

    @staticmethod
    def _jsonify_variables(variables):
        return orjson.loads(orjson.dumps(variables, default=RootNode._orjson_default))

    def _get_all_variables(self):
        return {
            k: v
            for key_value_pairs in self._recurse(
                lambda child: child._fragments.variables.items()
            )
            for k, v in key_value_pairs
        }

    def _get_all_outer_args(self: RootNode):
        return [
            outer_arg_submodel
            for outer_args_submodel in self._recurse(
                lambda node: node._fragments.outer_args
            )
            for outer_arg_submodel in outer_args_submodel
        ]

    @staticmethod
    def _orjson_default(obj):
        if isinstance(obj, (set, frozenset, GeneratorType)):
            return list(obj)
        elif isinstance(obj, BaseModel):
            return obj.dict()
        else:
            return jsonable_encoder(obj=obj)
