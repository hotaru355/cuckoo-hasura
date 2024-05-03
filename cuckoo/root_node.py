from __future__ import annotations

from io import BytesIO
from itertools import chain, islice
from logging import Logger
from types import GeneratorType
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

import orjson
from httpx import AsyncClient, Client, Response
from pydantic import BaseModel
from tenacity import AsyncRetrying, RetryError, Retrying

from .binary_tree_node import BinaryTreeNode
from .constants import CuckooConfig
from .cuckoo import Cuckoo
from .encoders import jsonable_encoder
from .errors import HasuraClientError, HasuraServerError
from .models import TMODEL
from .utils import in_brackets, to_compact_str, to_truncated_str


class RootNode(BinaryTreeNode[TMODEL]):
    VAR_NAME_BASE = "var"

    def __init__(
        self,
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._config = (
            Cuckoo.cuckoo_config(config)
            if config is not None
            else Cuckoo.cuckoo_config()
        )
        self._logger = logger
        self._var_name_counter: int = 0
        self._response: Optional[Response] = None
        self._response_data: Optional[dict[str, Any]] = None
        self._is_batch = False

        if session is not None and not isinstance(session, Client):
            raise ValueError("`session` argument is not an httpx.Client instance.")
        if session_async is not None and not isinstance(session_async, AsyncClient):
            raise ValueError(
                "`session_async` argument is not an httpx.AsyncClient instance."
            )
        self._session_override = session
        self._session_override_async = session_async

    def __str__(self):
        outer_args = self._get_all_outer_args()
        return f"""
            {self._fragments.query_name}{
                in_brackets(', '.join(outer_args), condition=bool(outer_args))
            } {{
                {" ".join(str(child) for child in self._children)}
            }}
        """

    @property
    def session(self):
        try:
            return next(
                sess
                for sess in [Cuckoo.session(), self._session_override]
                if sess is not None
            )
        except StopIteration:
            raise HasuraClientError(
                "No session provided. It is recommended to use "
                "`cuckoo.init(session=my_session)` for setting up a global session. "
                "Use the `session=my_session` argument in the query/mutation "
                "constructor for a one-time session."
            )

    @property
    def session_async(self):
        try:
            return next(
                sess
                for sess in [Cuckoo.session_async(), self._session_override_async]
                if sess is not None
            )
        except StopIteration:
            raise HasuraClientError(
                "No async session provided. It is recommended to use "
                "`cuckoo.init(session_async=my_session)` for setting up a global session. "
                "Use the `session_async=my_session` argument in the query/mutation "
                "constructor for a one-time session."
            )

    def _get_response(
        self,
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

    def _generate_var_name(self):
        self._var_name_counter += 1
        return f"{self.VAR_NAME_BASE}{self._var_name_counter}"

    def _process_response(self):
        response_json: dict[str, Any] = orjson.loads(self._response.content)
        if self._logger:
            response_text = to_truncated_str(self._response.text)
            request_text = to_truncated_str(self._response.request.content.decode())

        if ("error" in response_json) or ("errors" in response_json):
            errors: list[dict[str, str]] = (
                [response_json.get("error")] if "error" in response_json else []
            ) + response_json.get("errors", [])
            if self._logger:
                self._logger.error(
                    f"Query failed.  Request={request_text}, "
                    f"response={response_text}, errors={str(errors)}."
                )
            raise HasuraServerError(errors)
        elif "data" in response_json:
            if self._logger:
                self._logger.debug(
                    f"Query successful. Request={request_text}, "
                    f"response={response_text}."
                )
            self._response_data = response_json["data"]
        else:
            raise NotImplementedError(
                "Response did not contain any errors nor data. "
                f"Response={self._response.text}."
            )

    def _build_request(self):
        url = self._config["url"]
        if url is None:
            raise HasuraClientError(
                "Missing Hasura server URL. Setup an `HASURA_URL` environment variable "
                'or globally configure cuckoo with `cuckoo.init({"url": ""})`'
            )

        query = to_compact_str(str(self))
        variables = self._get_all_variables()
        json = {"query": query}
        if variables:
            json["variables"] = RootNode._jsonify_variables(variables)

        if self._logger:
            self._logger.debug(
                f"Request created. {query=}, variables={to_truncated_str(variables)}."
            )

        return self.session.build_request(
            method="POST",
            url=self._config["url"],
            headers=self._config["headers"],
            json=json,
        )

    def _execute(self, stream=False):
        request = self._build_request()
        if self._logger:
            if stream:
                self._logger.debug("Dispatching http streaming request.")
            else:
                self._logger.debug("Dispatching http request.")

        try:
            for attempt in Retrying(**self._config["retry"]):
                with attempt:
                    self._response = self.session.send(request=request, stream=stream)
                    self._response.raise_for_status()
                    if not stream:
                        self._process_response()
        except RetryError as err:
            raise HasuraServerError(repr(err))

    async def _execute_async(self):
        request = self._build_request()
        if self._logger:
            self._logger.debug("Dispatching asynchronous http request.")

        try:
            async for attempt in AsyncRetrying(**self._config["retry"]):
                with attempt:
                    self._response = await self.session_async.send(request=request)
                    self._response.raise_for_status()
                    self._process_response()
        except RetryError as err:
            raise HasuraServerError(repr(err))

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

    def _get_all_outer_args(self):
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


class IterByteIO(BytesIO):
    """Convert an `Iterable[bytes]` into a file-like (read-only) object.

    - uses `itertools.islice` internally to continuously read a chunk of bytes from the
        generator. This is faster than using a `while` loop to fill the buffer, although
        no performance testing has been conducted.

    Note: Specifically intended to be used with the `ijson.items()` function that calls
    the `read` and `readinto` methods only. Other use cases might call functions that
    are not implemented here.

    """

    def __init__(self, iterable: Iterable[bytes]):
        self.iter = chain.from_iterable(iterable)

    def readable(self):
        return True

    def readinto(self, buffer: bytearray):
        chunk = bytes(islice(self.iter, None, len(buffer)))
        chunk_length = len(chunk)
        buffer[:chunk_length] = chunk
        return chunk_length

    def read(self, chunk_size: Optional[int] = None):
        chunk = islice(self.iter, None, chunk_size)
        return bytes(chunk)
