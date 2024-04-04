from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from logging import Logger
from typing import Any, Generic, Optional, Type

from httpx import AsyncClient, Client

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import ORDER_BY, WHERE, CuckooConfig
from cuckoo.errors import MutationFailedError
from cuckoo.finalizers import (
    TFIN_MANY,
    TFIN_ONE,
    ReturningFinalizer,
    YieldingFinalizer,
)
from cuckoo.models import TBATCH_MODEL, TMODEL
from cuckoo.root_node import RootNode
from cuckoo.utils import to_sql_function_args


class InnerMutation(
    BinaryTreeNode[TMODEL],
    Generic[
        TMODEL,
        TFIN_ONE,
        TFIN_MANY,
    ],
):
    """
    The inner part of a query.

    Extends:
        - BinaryTreeNode: Provides properties and methods to create a binary tree.
    """

    def __init__(
        self,
        model: Type[TMODEL],
        finalizers: tuple[
            Type[TFIN_ONE],
            Type[TFIN_MANY],
        ],
        parent: Optional[BinaryTreeNode] = None,
        **kwargs,
    ):
        """
        Args:
            model (Type[TMODEL]): The model the query is resolving to
            parent (BinaryTreeNode, optional): The parent node. Defaults to None.
            base_model (Type[TMODEL_BASE], optional): The model that is used for
                aggregates on `min` and `max`. Defaults to UntypedModel.
            numeric_model (Type[TNUM_PROPS], optional): The model that is used for
                aggregates except `min` and `max`. Defaults to UntypedModel.
        """
        super().__init__(model=model, parent=parent, **kwargs)
        (
            self._one_finalizer,
            self._many_finalizer,
        ) = finalizers

    def one_function(
        self,
        function_name: str,
        *,
        args: Optional[dict[str, Any]] = {},
    ):
        inner_query = self._get_inner_mutation()
        function_name_with_schema = self._prepend_with_schema(function_name)
        inner_query._fragments.query_name = (
            f"{inner_query._query_alias}:{function_name_with_schema}"
        )
        inner_query._fragments.build_from_conditionals(
            args=(to_sql_function_args(args), function_name_with_schema),
        )

        return self._one_finalizer(
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
        inner_query = self._get_inner_mutation()
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

    def _get_inner_mutation(self):
        return (
            InnerMutation(
                model=self.model,
                parent=self,
                finalizers=(
                    self._one_finalizer,
                    self._many_finalizer,
                ),
            )
            if isinstance(self, Mutation)
            else self
        )

    def _build_one_model(self):
        data: dict[str, Any] = self._root._get_response(self._query_alias)
        if data is None:
            raise MutationFailedError()

        yield self.model(**data)

    def _build_many_models(self):
        for data in self._root._get_response(self._query_alias):
            yield self.model(**data)


class MutationBase(
    RootNode[TMODEL],
):
    def __init__(
        self,
        model: Type[TMODEL],
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ):
        super().__init__(
            model=model,
            config=config,
            session=session,
            session_async=session_async,
            logger=logger,
            **kwargs,
        )
        self._fragments.query_name = "mutation Mutation"

    @staticmethod
    @contextmanager
    def batch(
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        logger: Optional[Logger] = None,
    ):
        """
        Start a batch mutation in a `with` context. The mutation is executed on leaving
        the `with` block and only then will results be available.

        ```py
        with Mutation.batch() as BatchInsert, BatchUpdate, BatchDelete :
            num_author_rows = BatchInsert(Author)
                .many(data=author_data)
                .yield_affected_rows()
            articles = BatchUpdate(Article)
                .many(data=article_data, where={"created_at": { "_lt": "now" } })
                .yielding(["title"])
            next(num_author_rows) # ERROR! The query has not been executed yet!
        next(num_author_rows) # OK
        ```

        Args:
            config (dict, optional): the cuckoo config. Defaults to None.
            logger (Logger, optional): logger used internally. Defaults to None.

        Returns:
            Generator[tuple[type[Insert], type[Update], type[Delete]]]: the 3 mutation
            constructors. Any instance created by these is bound to the hasura
            transaction.
        """
        from cuckoo.delete import BatchDelete
        from cuckoo.insert import BatchInsert
        from cuckoo.update import BatchUpdate

        root = Mutation(
            model=object,  # hack
            config=config,
            session=session,
            logger=logger,
        )
        root._is_batch = True

        def insert(model: Type[TBATCH_MODEL]):
            return BatchInsert[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def update(model: Type[TBATCH_MODEL]):
            return BatchUpdate[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def delete(model: Type[TBATCH_MODEL]):
            return BatchDelete[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def mutation(model: Type[TBATCH_MODEL]):
            return BatchMutation[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        yield (insert, update, delete, mutation)

        root._execute()

    @staticmethod
    @asynccontextmanager
    async def batch_async(
        config: Optional[CuckooConfig] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
    ):
        from cuckoo.delete import BatchDelete
        from cuckoo.insert import BatchInsert
        from cuckoo.update import BatchUpdate

        root = Mutation(
            model=object,  # hack
            config=config,
            session_async=session_async,
            logger=logger,
        )
        root._is_batch = True

        def insert(model: Type[TBATCH_MODEL]):
            return BatchInsert[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def update(model: Type[TBATCH_MODEL]):
            return BatchUpdate[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def delete(model: Type[TBATCH_MODEL]):
            return BatchDelete[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        def mutation(model: Type[TBATCH_MODEL]):
            return BatchMutation[TBATCH_MODEL](
                parent=root,
                model=model,
            )

        yield (insert, update, delete, mutation)

        await root._execute_async()


class Mutation(
    MutationBase[TMODEL],
    InnerMutation[
        TMODEL,
        ReturningFinalizer[TMODEL, TMODEL],
        ReturningFinalizer[TMODEL, list[TMODEL]],
    ],
):
    def __init__(
        self,
        model: Type[TMODEL],
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ):
        super().__init__(
            model=model,
            config=config,
            session=session,
            session_async=session_async,
            logger=logger,
            finalizers=(
                ReturningFinalizer[TMODEL, TMODEL],
                ReturningFinalizer[TMODEL, list[TMODEL]],
            ),
            **kwargs,
        )
        self._fragments.query_name = "mutation Mutation"


class BatchMutation(
    InnerMutation[
        TMODEL,
        YieldingFinalizer[TMODEL],
        YieldingFinalizer[TMODEL],
    ],
):
    def __init__(
        self,
        parent: RootNode,
        model: Type[TMODEL],
        **kwargs,
    ):
        super().__init__(
            model=model,
            parent=parent,
            finalizers=(
                YieldingFinalizer[TMODEL],
                YieldingFinalizer[TMODEL],
            ),
            **kwargs,
        )
