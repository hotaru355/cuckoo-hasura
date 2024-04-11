from __future__ import annotations

from typing import (
    Any,
    Callable,
    Optional,
    Type,
    Union,
)

from typing_extensions import TypeAlias

from cuckoo.binary_tree_node import BinaryTreeNode
from cuckoo.constants import ORDER_BY, WHERE
from cuckoo.errors import HasuraClientError
from cuckoo.finalizers import (
    AggregateIncludeFinalizer,
    IncludeFinalizer,
)
from cuckoo.models import TMODEL


class Include(BinaryTreeNode[TMODEL]):
    def __init__(
        self,
        model: Type[TMODEL],
        field_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(model=model, parent=None, **kwargs)
        self._field_name = field_name

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

    def _get_relation_name(
        self,
        is_list=False,
    ):
        if self._field_name is not None:
            self._verify_field_is_model(self._field_name)
            return self._field_name

        def match_model_type(field_info: tuple[str, Any, bool]):
            (_, field_type, is_many_relation) = field_info
            return field_type is self.model and is_list == is_many_relation

        parent_field_names = [
            field_name
            for field_name, _, _ in filter(
                match_model_type, self._parent.model.fields(include_relations=True)
            )
        ]

        if len(parent_field_names) == 0:
            raise HasuraClientError(
                "Invalid sub-query. Could not find any reference to "
                + (f"List[{self.model}]" if is_list else f"{self.model}")
                + f" in {self._parent.model}"
            )
        if len(parent_field_names) > 1:
            raise HasuraClientError(
                f"Ambiguous sub query. Candidates: {parent_field_names}. "
                "Use the `field_name` argument to select one."
            )

        return parent_field_names.pop()

    def _verify_field_is_model(self, field_name):
        field_type = self._parent.model._get_field_type(field_name)
        if field_type is None:
            raise HasuraClientError(
                f"Invalid sub-query. The provided `field_name={field_name}` does "
                f"not exist on model `{self._parent.model}`."
            )
        if field_type is not self.model:
            raise HasuraClientError(
                f"Invalid sub-query. The provided model `{self.model}` "
                f"does not match expected model `{field_type}`."
            )


TINCLUDE: TypeAlias = Callable[[BinaryTreeNode], Include]
TCOLUMNS: TypeAlias = list[Union[str, TINCLUDE]]
