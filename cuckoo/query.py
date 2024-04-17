from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from logging import Logger
from typing import (
    Any,
    Generic,
    Optional,
    Type,
    cast,
)
from uuid import UUID

import ijson
from httpx import AsyncClient, Client

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import ORDER_BY, WHERE, CuckooConfig
from cuckoo.errors import RecordNotFoundError
from cuckoo.finalizers import (
    TFIN_AGGR,
    TFIN_MANY,
    TFIN_ONE,
    AggregateFinalizer,
    ReturningFinalizer,
    YieldingAggregateFinalizer,
    YieldingFinalizer,
)
from cuckoo.models import (
    TBATCH_MODEL,
    TBATCHMODEL_BASE,
    TBATCHNUM_PROPS,
    TMODEL,
    TMODEL_BASE,
    TNUM_PROPS,
    Aggregate,
    UntypedModel,
)
from cuckoo.root_node import IterByteIO, RootNode
from cuckoo.utils import to_sql_function_args


class InnerQuery(
    BinaryTreeNode[TMODEL],
    Generic[
        TMODEL,
        TFIN_ONE,
        TFIN_MANY,
        TFIN_AGGR,
        TMODEL_BASE,
        TNUM_PROPS,
    ],
):
    """The inner part of a query.

    Extends:
        - BinaryTreeNode: Provides properties and methods to create a binary tree.
    """

    def __init__(
        self,
        model: Type[TMODEL],
        finalizers: tuple[
            Type[TFIN_ONE],
            Type[TFIN_MANY],
            Type[TFIN_AGGR],
        ],
        parent: Optional[BinaryTreeNode] = None,
        base_model: Optional[Type[TMODEL_BASE]] = cast(Type[TMODEL_BASE], UntypedModel),
        numeric_model: Optional[Type[TNUM_PROPS]] = cast(
            Type[TNUM_PROPS], UntypedModel
        ),
        **kwargs,
    ):
        """
        Args:
            model (Type[TMODEL]): The model the query is resolving to
            parent (BinaryTreeNode, optional): The parent node. Defaults to None.
            finalizers (tuple[Type[TFIN_ONE], Type[TFIN_MANY], Type[TFIN_AGGR]],
                optional):
                The classes that end a query. Defaults to
                ( ReturningFinalizer[TMODEL], ReturningFinalizer[list[TMODEL]],
                AggregateFinalizer[Aggregate[TMODEL_BASE, TNUM_PROPS], TMODEL], ).
            base_model (Type[TMODEL_BASE], optional): The model that is used for
                aggregates on `min` and `max`. Defaults to UntypedModel.
            numeric_model (Type[TNUM_PROPS], optional): The model that is used for
                aggregates except `min` and `max`. Defaults to UntypedModel.
        """
        super().__init__(model=model, parent=parent, **kwargs)
        (
            self._one_by_pk_finalizer,
            self._many_finalizer,
            self._aggregate_finalizer,
        ) = finalizers
        self._base_model = base_model
        self._numeric_model = numeric_model

    def one_by_pk(
        self,
        uuid: UUID,
    ):
        """Build a query for finding a single model by its UUID.

        Args:
            uuid: The uuid of the record to query

        Returns:
            ReturningFinalizer: if outside of a `batch()`
            execution context
            YieldingFinalizer: if inside a `batch()`
            execution context

        Raises:
            NotFoundError: no matching record was found
        """

        inner_query = self._get_inner_query()
        var_name = self._root._generate_var_name()
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}: {inner_query._table_name}_by_pk"
        )
        inner_query._fragments.outer_args = [f"${var_name}: uuid!"]
        inner_query._fragments.inner_args = [f"uuid: ${var_name}"]
        inner_query._fragments.variables = {var_name: uuid}

        return self._one_by_pk_finalizer(
            node=inner_query,
            returning_fn=inner_query._build_one_model,
            gen_to_val={"returning": next},
        )

    def many(
        self,
        *,
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        """
        Build a query for a list of models.

        Args:
            where (WHERE, optional): The where filter, e.g.
                `{"name": {"_eq": "cuckoo"} }`. Defaults to None.
            distinct_on (set[str], optional): The field(s) required to be distinct.
                Defaults to None.
            limit (int, optional): The maximum number of records returned.
                Defaults to None.
            offset (int, optional): The number of records to offset. Defaults to None.
            order_by (ORDER_BY, optional): The order condition, e.g. `{"age": "desc"}`.
                Defaults to None.

        Returns:
            - `ReturningDirectFinalizer[list[TMODEL]]`: for queries outside of a
                `batch()` execution context
            - `YieldingDirectFinalizer[list[TMODEL]]`: for queries inside a `batch()`
                execution context
        """

        inner_query = self._get_inner_query()
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{inner_query._table_name}"
        )
        inner_query._fragments.build_from_conditionals(
            where=where,
            distinct_on=distinct_on,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

        return self._many_finalizer(
            node=inner_query,
            streaming_fn=inner_query._build_many_models_stream,  # for `yielding` and `returning`
            returning_fn=inner_query._build_many_models,  # for `returning_async` only
            gen_to_val={"returning": list},
        )

    def aggregate(
        self,
        *,
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        """
        From the filtered and ordered result set, get one or more aggregate value.

        Args:
            where (WHERE, optional): _description_. Defaults to None.
            distinct_on (set[str], optional): _description_. Defaults to None.
            limit (int, optional): _description_. Defaults to None.
            offset (int, optional): _description_. Defaults to None.
            order_by (ORDER_BY, optional): _description_. Defaults to None.

        Raises:
            RecordNotFoundError: _description_

        Returns:
            _type_: _description_
        """

        inner_query = self._get_inner_query()
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{inner_query._table_name}_aggregate"
        )
        inner_query._fragments.build_from_conditionals(
            where=where,
            distinct_on=distinct_on,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )

        return self._aggregate_finalizer(
            inner_query,
            aggregate_fn=inner_query._build_aggregate,
            nodes_fn=inner_query._build_nodes,
        )

    def one_function(
        self,
        function_name: str,
        *,
        args: Optional[dict[str, Any]] = {},
    ):
        inner_query = self._get_inner_query()
        function_name_with_schema = self._prepend_with_schema(function_name)
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{function_name_with_schema}"
        )
        inner_query._fragments.build_from_conditionals(
            args=(to_sql_function_args(args), function_name_with_schema),
        )

        return self._one_by_pk_finalizer(
            node=inner_query,
            returning_fn=inner_query._build_one_model,
            gen_to_val={"returning": next},
        )

    def many_function(
        self,
        function_name: str,
        *,
        args: Optional[dict[str, Any]] = {},
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        inner_query = self._get_inner_query()
        function_name_with_schema = self._prepend_with_schema(function_name)
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{function_name_with_schema}"
        )
        inner_query._fragments.build_from_conditionals(
            where=where,
            distinct_on=distinct_on,
            limit=limit,
            offset=offset,
            order_by=order_by,
            args=(to_sql_function_args(args), function_name_with_schema),
        )

        return self._many_finalizer(
            node=inner_query,
            returning_fn=inner_query._build_many_models,
            gen_to_val={"returning": list},
        )

    def aggregate_function(
        self,
        function_name: str,
        *,
        args: Optional[dict[str, Any]] = {},
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        inner_query = self._get_inner_query()
        function_name_with_schema = self._prepend_with_schema(function_name)
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{function_name_with_schema}_aggregate"
        )
        inner_query._fragments.build_from_conditionals(
            where=where,
            distinct_on=distinct_on,
            limit=limit,
            offset=offset,
            order_by=order_by,
            args=(to_sql_function_args(args), function_name_with_schema),
        )

        return self._aggregate_finalizer(
            inner_query,
            aggregate_fn=inner_query._build_aggregate,
            nodes_fn=inner_query._build_nodes,
        )

    def _build_one_model(self):
        data: dict[str, Any] = self._root._get_response(self._query_alias)
        if data is None:
            raise RecordNotFoundError()

        yield self.model(**data)

    def _build_many_models(self):
        data_list: list[dict] = self._root._get_response(self._query_alias)
        for data in data_list:
            yield self.model(**data)

    def _build_many_models_stream(self):
        for item in ijson.items(
            IterByteIO(self._root._response.iter_bytes()),
            f"data.{self._query_alias}.item",
        ):
            yield self.model(**item)

    def _build_aggregate(self):
        response: dict[str, Any] = self._root._get_response(
            self._query_alias, "aggregate"
        )

        yield Aggregate[self._base_model, self._numeric_model](**response)
        # yield Aggregate[TMODEL_BASE, TNUM_PROPS](**response)

    def _build_nodes(self):
        data_list: list[dict[str, Any]] = self._root._get_response(
            self._query_alias, "nodes"
        )

        for data in data_list:
            yield self.model(**data)

    def _get_inner_query(self):
        return (
            InnerQuery(
                model=self.model,
                parent=self,
                finalizers=(
                    self._one_by_pk_finalizer,
                    self._many_finalizer,
                    self._aggregate_finalizer,
                ),
            )
            if isinstance(self, Query)
            else self
        )


class Query(
    RootNode[TMODEL],
    InnerQuery[
        TMODEL,
        ReturningFinalizer[TMODEL, TMODEL],
        ReturningFinalizer[TMODEL, list[TMODEL]],
        AggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
        TMODEL_BASE,
        TNUM_PROPS,
    ],
):
    """Query builder for retrieving one, many, or an aggregate of models.

    Extends:
        RootNode: provides functionality to execute a query.
        InnerQuery: provides functionality to specify the kind of query to be created.
    """

    def __init__(
        self,
        model: Type[TMODEL],
        *,
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
        base_model: Type[TMODEL_BASE] = cast(Type[TMODEL_BASE], UntypedModel),
        numeric_model: Type[TNUM_PROPS] = cast(Type[TNUM_PROPS], UntypedModel),
    ):
        """
        Create a new query builder.

        Args:
            model (Type[TMODEL]): The model the query is resolving to
            config (dict, optional): The hasura connection settings and other
                configuration. Defaults to None.
            logger (Optional[Logger], optional): The logger. Defaults to None.
            base_model (Type[TMODEL_BASE], optional): The model that is used for
                aggregates on `min` and `max`. Defaults to UntypedModel.
            numeric_model (Type[TNUM_PROPS], optional): The model that is used for
                aggregates except `min` and `max`. Defaults to UntypedModel.
        """
        super().__init__(
            model=model,
            config=config,
            session=session,
            session_async=session_async,
            logger=logger,
            base_model=base_model,
            numeric_model=numeric_model,
            finalizers=(
                ReturningFinalizer[TMODEL, TMODEL],
                ReturningFinalizer[TMODEL, list[TMODEL]],
                AggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
            ),
        )
        self._fragments.query_name = "query Query"

    @staticmethod
    @contextmanager
    def batch(
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        logger: Optional[Logger] = None,
    ):
        root: Query = Query(
            model=object,
            config=config,
            session=session,
            logger=logger,
        )
        root._is_batch = True

        def query(
            model: Type[TBATCH_MODEL],
            base_model: Type[TBATCHMODEL_BASE] = cast(
                Type[TBATCHMODEL_BASE], UntypedModel
            ),
            numeric_model: Type[TBATCHNUM_PROPS] = cast(
                Type[TBATCHNUM_PROPS], UntypedModel
            ),
        ):
            return BatchQuery[TBATCH_MODEL, TBATCHMODEL_BASE, TBATCHNUM_PROPS](
                parent=root,
                model=model,
                base_model=base_model,
                numeric_model=numeric_model,
            )

        yield query

        root._execute()

    @staticmethod
    @asynccontextmanager
    async def batch_async(
        config: Optional[CuckooConfig] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
    ):
        root: Query = Query(
            model=object,
            config=config,
            session_async=session_async,
            logger=logger,
        )
        root._is_batch = True

        def query(
            model: Type[TBATCH_MODEL],
            base_model: Type[TBATCHMODEL_BASE] = cast(
                Type[TBATCHMODEL_BASE], UntypedModel
            ),
            numeric_model: Type[TBATCHNUM_PROPS] = cast(
                Type[TBATCHNUM_PROPS], UntypedModel
            ),
        ):
            return BatchQuery[TBATCH_MODEL, TBATCHMODEL_BASE, TBATCHNUM_PROPS](
                parent=root,
                model=model,
                base_model=base_model,
                numeric_model=numeric_model,
            )

        yield query

        await root._execute_async()


class BatchQuery(
    InnerQuery[
        TMODEL,
        YieldingFinalizer[TMODEL],
        YieldingFinalizer[TMODEL],
        YieldingAggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
        TMODEL_BASE,
        TNUM_PROPS,
    ],
):
    def __init__(
        self,
        parent: RootNode,
        model: Type[TMODEL],
        base_model: Optional[Type[TMODEL_BASE]] = None,
        numeric_model: Optional[Type[TNUM_PROPS]] = None,
        **kwargs,
    ):
        super().__init__(
            model=model,
            parent=parent,
            finalizers=(
                YieldingFinalizer[TMODEL],
                YieldingFinalizer[TMODEL],
                YieldingAggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
            ),
            base_model=base_model,
            numeric_model=numeric_model,
            **kwargs,
        )
