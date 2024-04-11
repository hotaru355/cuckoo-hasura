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

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import HASURA_DEFAULT_CONFIG, RETRY_DEFAULT_CONFIG, CuckooConfig
from cuckoo.encoders import jsonable_encoder
from cuckoo.errors import HasuraClientError, HasuraServerError
from cuckoo.models import TMODEL
from cuckoo.utils import in_brackets, to_compact_str, to_truncated_str


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
        self._config: CuckooConfig = {
            **HASURA_DEFAULT_CONFIG,  # type: ignore
            "retry": RETRY_DEFAULT_CONFIG,
            **(config if config else {}),
        }
        self._logger = logger
        self._var_name_counter: int = 0
        self._response: Optional[Response] = None
        self._response_data: Optional[dict[str, Any]] = None
        self._is_batch = False

        self._close_session = False
        if session is None:
            self._close_session = True
            self._session = Client(timeout=None)
        else:
            self._session = session

        self._close_session_async = False
        if session_async is None:
            self._close_session_async = True
            self._session_async = AsyncClient(timeout=None)
        else:
            self._session_async = session_async

    def __str__(self):
        outer_args = self._get_all_outer_args()
        return f"""
            {self._fragments.query_name}{
                in_brackets(', '.join(outer_args), condition=bool(outer_args))
            } {{
                {" ".join(str(child) for child in self._children)}
            }}
        """

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
        query = to_compact_str(str(self))
        variables = self._get_all_variables()
        json = {"query": query}
        if variables:
            json["variables"] = RootNode._jsonify_variables(variables)

        if self._logger:
            self._logger.debug(
                f"Request created. {query=}, variables={to_truncated_str(variables)}."
            )

        return self._session.build_request(
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
                    self._response = self._session.send(request=request, stream=stream)
                    self._response.raise_for_status()
                    if not stream:
                        self._process_response()
        except RetryError as err:
            raise HasuraServerError(repr(err))
        finally:
            if self._close_session and self._session is not None:
                self._session.close()

    async def _execute_async(self):
        request = self._build_request()
        if self._logger:
            self._logger.debug("Dispatching asynchronous http request.")

        try:
            async for attempt in AsyncRetrying(**self._config["retry"]):
                with attempt:
                    self._response = await self._session_async.send(request=request)
                    self._response.raise_for_status()
                    self._process_response()
        except RetryError as err:
            raise HasuraServerError(repr(err))
        finally:
            if self._close_session_async and self._session_async is not None:
                await self._session_async.aclose()

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
