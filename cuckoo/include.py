from __future__ import annotations
from functools import reduce
from typing import (
    Callable,
    Optional,
    Type,
    Union,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import TypeAlias

from pydantic.fields import ModelField

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import ORDER_BY, WHERE
from cuckoo.errors import HasuraClientError
from cuckoo.finalizers import (
    AggregateIncludeFinalizer,
    IncludeFinalizer,
)
from cuckoo.models import TMODEL, HasuraTableModel


class Include(BinaryTreeNode[TMODEL]):
    def __init__(
        self,
        model: Type[TMODEL],
        key: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(model=model, parent=None, **kwargs)
        self._key = key

    def one(self):
        def bind_and_configure(parent: BinaryTreeNode):
            self._bind_to_parent(parent)
            self._fragments.query_name = self._get_relation_name()
            return self

        return IncludeFinalizer(node=self, finalize_fn=bind_and_configure)

    def many(
        self,
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        def bind_and_configure(parent: BinaryTreeNode):
            self._bind_to_parent(parent)
            self._fragments.query_name = self._get_relation_name(is_list=True)
            self._fragments.build_from_conditionals(
                where=where,
                distinct_on=distinct_on,
                limit=limit,
                offset=offset,
                order_by=order_by,
            )
            return self

        return IncludeFinalizer(node=self, finalize_fn=bind_and_configure)

    def aggregate(
        self,
        where: Optional[WHERE] = None,
        distinct_on: Optional[set[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[ORDER_BY] = None,
    ):
        def bind_and_configure(parent: BinaryTreeNode):
            self._bind_to_parent(parent)
            self._fragments.query_name = (
                f"{self._get_relation_name(is_list=True)}_aggregate"
            )
            self._fragments.build_from_conditionals(
                where=where,
                distinct_on=distinct_on,
                limit=limit,
                offset=offset,
                order_by=order_by,
            )
            return self

        return AggregateIncludeFinalizer(node=self, finalize_fn=bind_and_configure)

    def _get_relation_name(self, is_list=False):
        parent_model = cast(HasuraTableModel, self._parent.model)
        if self._key is not None:
            parent_key, model_of_key = next(
                filter(
                    lambda field: field[0] == self._key, parent_model.__fields__.items()
                ),
                (None, None),
            )
            if parent_key is None or model_of_key is None:
                raise HasuraClientError(
                    f"Invalid sub-query. The provided key `{self._key}` not found on "
                    f"model `{parent_model}`"
                )
            elif model_of_key.type_ is not self.model:
                raise HasuraClientError(
                    f"Invalid sub-query. The provided model `{self.model}` "
                    f"does not match expected model `{model_of_key.type_}`"
                )
            return parent_key

        def find_model_attrs_by_type(
            matching_keys: list[str], dict_items: tuple[str, ModelField]
        ):
            key, value = dict_items
            if value.type_ is self.model:
                if is_list:
                    if get_origin(value.annotation) is Union and any(
                        [get_origin(sub) is list for sub in get_args(value.annotation)]
                    ):
                        matching_keys.append(key)
                else:
                    matching_keys.append(key)
            return matching_keys

        matching_keys: list[str] = reduce(
            find_model_attrs_by_type,
            parent_model.__fields__.items(),
            [],
        )

        if len(matching_keys) == 0:
            raise HasuraClientError(
                "Invalid sub-query. Could not find any reference to "
                + (f"List[{self.model}]" if is_list else f"{self.model}")
                + f" in {parent_model}"
            )
        elif len(matching_keys) == 1:
            return matching_keys[0]
        else:
            raise HasuraClientError(
                f"Ambiguous sub query. Candidates: {matching_keys}. "
                "Use the `key` argument to select one."
            )


TINCLUDE: TypeAlias = Callable[[BinaryTreeNode], Include]
TCOLUMNS: TypeAlias = list[Union[str, TINCLUDE]]
