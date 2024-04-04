from __future__ import annotations

from logging import Logger
from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    Type,
    TypedDict,
)

from httpx import AsyncClient, Client
from typing_extensions import NotRequired

from cuckoo.constants import CuckooConfig
from cuckoo.errors import InsertFailedError
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


class OnConflict(TypedDict):
    constraint: str
    update_columns: list[str]
    where: NotRequired[dict[str, Any]]


class InnerInsert(
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

    def one(
        self,
        data: dict[str, Any],
        on_conflict: Optional[OnConflict] = None,
    ) -> TFIN_ONE:
        object_var_name = self._root._generate_var_name()
        inner_insert = self._get_inner_insert()
        inner_insert._fragments.query_name = (
            f"{inner_insert._query_alias}:insert_{inner_insert._table_name}_one"
        )
        inner_insert._fragments.outer_args = [
            f"${object_var_name}: {inner_insert._table_name}_insert_input!"
        ]
        inner_insert._fragments.inner_args = [f"object: ${object_var_name}"]
        inner_insert._fragments.variables = {object_var_name: data}
        inner_insert._fragments.build_from_conditionals(on_conflict=on_conflict)

        return self._one_finalizer(
            node=inner_insert,
            returning_fn=inner_insert._build_one_model,
            gen_to_val={"returning": next},
        )

    def many(
        self,
        data: list[dict[str, Any]],
        on_conflict: Optional[OnConflict] = None,
    ) -> TFIN_MANY:
        objects_var_name = self._root._generate_var_name()
        inner_insert = self._get_inner_insert()
        inner_insert._fragments.query_name = (
            f"{inner_insert._query_alias}:insert_{inner_insert._table_name}"
        )
        inner_insert._fragments.outer_args = [
            f"${objects_var_name}: [{inner_insert._table_name}_insert_input!]!"
        ]
        inner_insert._fragments.inner_args = [f"objects: ${objects_var_name}"]
        inner_insert._fragments.variables = {objects_var_name: data}
        inner_insert._fragments.build_from_conditionals(on_conflict=on_conflict)

        return self._many_finalizer(
            node=inner_insert,
            returning_fn=inner_insert._build_many_models,
            affected_rows_fn=inner_insert._get_rows,
            returning_with_rows_fn=lambda: (
                inner_insert._build_many_models(),
                inner_insert._get_rows(),
            ),
            gen_to_val={
                "returning": list,
                "affected_rows": next,
                "returning_with_rows": lambda tup: (list(tup[0]), next(tup[1])),
            },
        )

    def _build_one_model(self):
        data = self._root._get_response(self._query_alias)
        if data is None:
            raise InsertFailedError(
                "The operation did not return a record. Possible reason: "
                "The `where` clause of `on_conflict` did not match any records."
            )

        yield self.model(**data)

    def _build_many_models(self):
        for data in self._root._get_response(self._query_alias, "returning"):
            yield self.model(**data)

    def _get_rows(self) -> int:
        rows = self._root._get_response(self._query_alias, "affected_rows")
        yield rows

    def _get_inner_insert(self):
        return (
            InnerInsert(
                model=self.model,
                parent=self,
                finalizers=(self._one_finalizer, self._many_finalizer),
            )
            if isinstance(self, Insert)
            else self
        )


class Insert(
    MutationBase[TMODEL],
    InnerInsert[
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
            config=config,
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
            session=session,
            session_async=session_async,
            logger=logger,
        )


class BatchInsert(
    InnerInsert[
        TMODEL,
        YieldingFinalizer[TMODEL],
        YieldingAffectedRowsFinalizer[
            TMODEL,
            tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
            int,
        ],
    ],
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
