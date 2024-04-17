from __future__ import annotations

from logging import Logger
from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    Type,
)

from httpx import AsyncClient, Client

from cuckoo.constants import DISTINCT_UPDATES, WHERE, CuckooConfig
from cuckoo.errors import RecordNotFoundError
from cuckoo.finalizers import (
    TFIN_MANY,
    TFIN_MANY_DISTINCT,
    TFIN_ONE,
    AffectedRowsFinalizer,
    ReturningFinalizer,
    YieldingAffectedRowsFinalizer,
    YieldingFinalizer,
)
from cuckoo.models import TMODEL
from cuckoo.mutation import MutationBase
from cuckoo.root_node import BinaryTreeNode, RootNode


class InnerUpdate(
    BinaryTreeNode[TMODEL],
    Generic[TMODEL, TFIN_ONE, TFIN_MANY, TFIN_MANY_DISTINCT],
):
    def __init__(
        self,
        model: Type[TMODEL],
        finalizers: tuple[Type[TFIN_ONE], Type[TFIN_MANY], Type[TFIN_MANY_DISTINCT]],
        parent: Optional[BinaryTreeNode] = None,
        **kwargs,
    ):
        super().__init__(model=model, parent=parent, **kwargs)
        (
            self._one_finalizer,
            self._many_finalizer,
            self._many_distinct_finalizer,
        ) = finalizers

    def one_by_pk(
        self,
        pk_columns: dict,
        data: Optional[dict] = None,
        inc: Optional[dict] = None,
        append: Optional[dict] = None,
        prepend: Optional[dict] = None,
        delete_key: Optional[dict] = None,
        delete_elem: Optional[dict] = None,
        delete_at_path: Optional[dict] = None,
    ) -> TFIN_ONE:
        self._assert_update_args(
            data, inc, prepend, append, delete_key, delete_elem, delete_at_path
        )

        inner_update = self._get_inner_update()
        inner_update._fragments.query_name = (
            f"{inner_update._query_alias}:update_{inner_update._table_name}_by_pk"
        )
        inner_update._fragments.build_from_conditionals(
            pk_columns=pk_columns,
            data=data,
            inc=inc,
            append=append,
            prepend=prepend,
            delete_key=delete_key,
            delete_elem=delete_elem,
            delete_at_path=delete_at_path,
        )

        return self._one_finalizer(
            node=inner_update,
            returning_fn=inner_update._build_one_model,
            gen_to_val={"returning": next},
        )

    def many(
        self,
        where: WHERE,
        data: Optional[dict] = None,
        inc: Optional[dict] = None,
        append: Optional[dict] = None,
        prepend: Optional[dict] = None,
        delete_key: Optional[dict] = None,
        delete_elem: Optional[dict] = None,
        delete_at_path: Optional[dict] = None,
    ) -> TFIN_MANY:
        self._assert_update_args(
            data, inc, prepend, append, delete_key, delete_elem, delete_at_path
        )

        inner_update = self._get_inner_update()
        inner_update._fragments.query_name = (
            f"{inner_update._query_alias}:update_{inner_update._table_name}"
        )
        inner_update._fragments.build_from_conditionals(
            data=data,
            where_req=where,
            delete_at_path=delete_at_path,
            inc=inc,
            append=append,
            prepend=prepend,
            delete_elem=delete_elem,
            delete_key=delete_key,
        )

        return self._many_finalizer(
            node=inner_update,
            returning_fn=inner_update._build_many_models,
            affected_rows_fn=inner_update._get_rows,
            returning_with_rows_fn=lambda: (
                inner_update._build_many_models(),
                inner_update._get_rows(),
            ),
            gen_to_val={
                "returning": list,
                "affected_rows": next,
                "returning_with_rows": lambda tup: (list(tup[0]), next(tup[1])),
            },
        )

    def many_distinct(
        self,
        updates: DISTINCT_UPDATES,
    ) -> TFIN_MANY_DISTINCT:
        inner_update = self._get_inner_update()
        inner_update._fragments.query_name = (
            f"{inner_update._query_alias}:update_{inner_update._table_name}_many"
        )
        inner_update._fragments.build_from_conditionals(updates=updates)

        return self._many_distinct_finalizer(
            node=inner_update,
            returning_fn=inner_update._build_models_distinct,
            affected_rows_fn=inner_update._get_rows_distinct,
            returning_with_rows_fn=inner_update._build_models_and_rows_distinct,
            gen_to_val={
                "returning": lambda data_list: [list(data) for data in data_list],
                "affected_rows": list,
                "returning_with_rows": lambda data_list: [
                    (list(data[0]), data[1]) for data in data_list
                ],
            },
        )

    def _build_one_model(self):
        data: dict[str, Any] = self._root._get_response(self._query_alias)
        if data is None:
            raise RecordNotFoundError("Record to update was not found.")

        yield self.model(**data)

    def _build_many_models(self):
        for data in self._root._get_response(self._query_alias, "returning"):
            yield self.model(**data)

    def _get_rows(self):
        rows: int = self._root._get_response(self._query_alias, "affected_rows")
        yield rows

    def _build_models_distinct(self):
        for data_list in self._root._get_response(self._query_alias):
            yield self._build_model_from_list(data_list)

    def _get_rows_distinct(self):
        for data_list in self._root._get_response(self._query_alias):
            yield data_list["affected_rows"]

    def _build_models_and_rows_distinct(self):
        for data_list in self._root._get_response(self._query_alias):
            yield (self._build_model_from_list(data_list), data_list["affected_rows"])

    def _build_model_from_list(self, data_list: dict):
        for data in data_list["returning"]:
            yield self.model(**data)

    def _get_inner_update(self):
        return (
            InnerUpdate(
                model=self.model,
                parent=self,
                finalizers=(
                    self._one_finalizer,
                    self._many_finalizer,
                    self._many_distinct_finalizer,
                ),
            )
            if isinstance(self, Update)
            else self
        )

    def _assert_update_args(self, *args):
        if not any([*args]):
            raise ValueError(
                "Missing argument. At least one argument is required: data, inc, "
                "prepend, append, delete_key, delete_elem, delete_at_path"
            )


class Update(
    MutationBase[TMODEL],
    InnerUpdate[
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
        AffectedRowsFinalizer[
            Generator[TMODEL, None, None],
            list[list[TMODEL]],
            Generator[tuple[Generator[TMODEL, None, None], int], None, None],
            list[tuple[list[TMODEL], int]],
            int,
            list[int],
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
                AffectedRowsFinalizer[
                    Generator[TMODEL, None, None],
                    list[list[TMODEL]],
                    Generator[tuple[Generator[TMODEL, None, None], int], None, None],
                    list[tuple[list[TMODEL], int]],
                    int,
                    list[int],
                ],
            ),
            session=session,
            session_async=session_async,
            logger=logger,
        )


class BatchUpdate(
    InnerUpdate[
        TMODEL,
        YieldingFinalizer[TMODEL],
        YieldingAffectedRowsFinalizer[
            TMODEL,
            tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
            int,
        ],
        YieldingAffectedRowsFinalizer[
            Generator[TMODEL, None, None],
            Generator[tuple[Generator[TMODEL, None, None], int], None, None],
            int,
        ],
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
                YieldingAffectedRowsFinalizer[
                    TMODEL,
                    tuple[Generator[TMODEL, None, None], Generator[int, None, None]],
                    int,
                ],
                YieldingAffectedRowsFinalizer[
                    Generator[TMODEL, None, None],
                    Generator[tuple[Generator[TMODEL, None, None], int], None, None],
                    int,
                ],
            ),
            **kwargs,
        )
