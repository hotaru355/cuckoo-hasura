from __future__ import annotations

from logging import Logger
from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    Type,
)
from uuid import UUID

from httpx import AsyncClient, Client

from cuckoo.constants import WHERE, CuckooConfig
from cuckoo.errors import RecordNotFoundError
from cuckoo.finalizers import (
    TFIN_MANY,
    TFIN_ONE,
    AffectedRowsFinalizer,
    ReturningFinalizer,
    YieldingAffectedRowsFinalizer,
    YieldingFinalizer,
)
from cuckoo.models import TMODEL
from cuckoo.mutation import MutationBase
from cuckoo.root_node import BinaryTreeNode, RootNode


class InnerDelete(
    BinaryTreeNode[TMODEL],
    Generic[TMODEL, TFIN_ONE, TFIN_MANY],
):
    def __init__(
        self,
        model: Type[TMODEL],
        finalizers: tuple[Type[TFIN_ONE], Type[TFIN_MANY]],
        parent: Optional[BinaryTreeNode] = None,
        **kwargs,
    ):
        super().__init__(model=model, parent=parent, **kwargs)
        self._one_finalizer, self._many_finalizer = finalizers

    def one_by_pk(
        self,
        uuid: UUID,
    ) -> TFIN_ONE:
        inner_delete = self._get_inner_delete()
        var_name = self._root._generate_var_name()

        inner_delete._fragments.query_name = (
            f"{inner_delete._query_alias}: delete_{inner_delete._table_name}_by_pk"
        )
        inner_delete._fragments.outer_args = [f"${var_name}: uuid!"]
        inner_delete._fragments.inner_args = [f"uuid: ${var_name}"]
        inner_delete._fragments.variables = {var_name: uuid}

        return self._one_finalizer(
            node=inner_delete,
            returning_fn=inner_delete._build_one_model,
            gen_to_val={"returning": next},
        )

    def many(
        self,
        where: WHERE,
    ) -> TFIN_MANY:
        inner_delete = self._get_inner_delete()

        inner_delete._fragments.query_name = (
            f"{inner_delete._query_alias}: delete_{inner_delete._table_name}"
        )
        inner_delete._fragments.build_from_conditionals(
            where_req=where,
        )

        return self._many_finalizer(
            node=inner_delete,
            returning_fn=inner_delete._build_many_models,
            affected_rows_fn=inner_delete._get_rows,
            returning_with_rows_fn=lambda: (
                inner_delete._build_many_models(),
                inner_delete._get_rows(),
            ),
            gen_to_val={
                "returning": list,
                "affected_rows": next,
                "returning_with_rows": lambda tup: (list(tup[0]), next(tup[1])),
            },
        )

    def _build_one_model(self):
        data: dict[str, Any] = self._root._get_response(self._query_alias)
        if data is None:
            raise RecordNotFoundError()

        yield self.model(**data)

    def _build_many_models(self):
        for data in self._root._get_response(self._query_alias, "returning"):
            yield self.model(**data)

    def _get_rows(self):
        rows: int = self._root._get_response(self._query_alias, "affected_rows")
        yield rows

    def _get_inner_delete(self):
        return (
            InnerDelete(
                model=self.model,
                parent=self,
                finalizers=(self._one_finalizer, self._many_finalizer),
            )
            if isinstance(self, Delete)
            else self
        )


class Delete(
    MutationBase[TMODEL],
    InnerDelete[
        TMODEL,
        ReturningFinalizer[TMODEL, TMODEL],
        AffectedRowsFinalizer[
            TMODEL,
            list[TMODEL],
            tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
            tuple[list[TMODEL], int],
            int,
            int,
        ],
    ],
):
    def __init__(
        self,
        model: Type[TMODEL],
        *,
        config: Optional[CuckooConfig] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
    ):
        super().__init__(
            model=model,
            finalizers=(
                ReturningFinalizer[TMODEL, TMODEL],
                AffectedRowsFinalizer[
                    TMODEL,
                    list[TMODEL],
                    tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
                    tuple[list[TMODEL], int],
                    int,
                    int,
                ],
            ),
            config=config,
            session=session,
            session_async=session_async,
            logger=logger,
        )


class BatchDelete(
    InnerDelete[
        TMODEL,
        YieldingFinalizer[TMODEL],
        YieldingAffectedRowsFinalizer[
            TMODEL,
            tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
            int,
        ],
    ],
    Generic[TMODEL],
):
    def __init__(
        self,
        parent: RootNode,
        model: Type[TMODEL],
    ):
        super().__init__(
            model=model,
            parent=parent,
            finalizers=(
                YieldingFinalizer[TMODEL],
                YieldingAffectedRowsFinalizer[
                    TMODEL,
                    tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
                    int,
                ],
            ),
        )
