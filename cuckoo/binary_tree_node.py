from __future__ import annotations

import inspect
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

from cuckoo.constants import DISTINCT_UPDATES, ORDER_BY, WHERE
from cuckoo.models import TMODEL, TableModel
from cuckoo.utils import BracketStyle, in_brackets

if TYPE_CHECKING:
    from cuckoo.finalizers import AggregatesDict, CountDict
    from cuckoo.include import TCOLUMNS
    from cuckoo.root_node import HasuraClientError, RootNode

"""The `BinaryTreeNode[TMODEL]` with its data class `GraphQLFragments`.

The `BinaryTreeNode[TMODEL]` represents a node in a binary tree and it uses an instance
of the `GraphQLFragments` class for storing its data. The `GraphQLFragments` data class
contains a list of at least one response key class.
"""


class ColumnResponseKey:
    def __init__(self, columns: TCOLUMNS):
        self._columns = columns

    def __str__(self):
        return " ".join(str(column) for column in self._columns)


class ReturningResponseKey(ColumnResponseKey):
    def __str__(self):
        return f"""
            returning {{
                {super().__str__()}
            }}
        """


class AggregateResponseKey:
    def __init__(
        self,
        aggregates: AggregatesDict,
    ):
        self._aggregates = aggregates

    @staticmethod
    def as_gql(obj):
        if isinstance(obj, dict):
            return in_brackets(
                " ".join(
                    f"{key} {AggregateResponseKey.as_gql(value)}"
                    for key, value in obj.items()
                ),
                BracketStyle.CURLY,
            )
        elif isinstance(obj, set):
            return in_brackets(
                " ".join(AggregateResponseKey.as_gql(value) for value in obj),
                BracketStyle.CURLY,
            )
        elif isinstance(obj, (str, int, float)) and not isinstance(obj, bool):
            return obj
        else:
            raise ValueError("Cannot convert to GraphQL: ", obj)

    def __str__(self):
        return f"""
            aggregate {{
                {" ".join(
                    f"{field} {AggregateResponseKey.as_gql(obj)}"
                    for field, obj in self._aggregates.items()
                    if obj is not None
                )}
            }}
        """


class AffectedRowsResponseKey:
    def __str__(self):
        return "affected_rows"


class NodesResponseKey(ColumnResponseKey):
    def __str__(self):
        return f"""
            nodes {{
                {super().__str__()}
            }}
        """


class GraphQLFragments(Generic[TMODEL]):
    """
    Data class of the `BinaryTreeNode`, that stores all string fragments required for
    producing a GraphQL query or mutation. See `__str__` method for how they get
    arranged.
    """

    def __init__(
        self,
        node: BinaryTreeNode[TMODEL],
    ):
        self._node = node
        self.query_name: str
        self.outer_args: list[str] = []
        self.inner_args: list[str] = []
        self.response_keys: list[
            Union[
                ColumnResponseKey,
                ReturningResponseKey,
                AggregateResponseKey,
                AffectedRowsResponseKey,
                NodesResponseKey,
            ]
        ] = []
        self.variables: dict[str, Any] = {}

    def build_from_conditionals(
        self,
        append: Optional[dict] = None,
        data: Optional[dict] = None,
        delete_at_path: Optional[dict] = None,
        delete_elem: Optional[dict] = None,
        delete_key: Optional[dict] = None,
        distinct_on: Optional[set[str]] = None,
        inc: Optional[dict] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        on_conflict: Optional[dict[str, str]] = None,
        order_by: Optional[ORDER_BY] = None,
        pk_columns: Optional[dict] = None,
        prepend: Optional[dict] = None,
        updates: Optional[DISTINCT_UPDATES] = None,
        where_req: Optional[WHERE] = None,
        where: Optional[WHERE] = None,
        args: Optional[tuple[dict, str]] = None,  # (args_dict, function_name)
    ):
        table_name = self._node._get_table_name_by_model()
        fragments_by_arg = filter(
            lambda arg: arg[0] is not None,
            [
                (append, f"{table_name}_append_input", "_append"),
                (data, f"{table_name}_set_input", "_set"),
                (
                    delete_at_path,
                    f"{table_name}_delete_at_path_input",
                    "_delete_at_path",
                ),
                (delete_elem, f"{table_name}_delete_elem_input", "_delete_elem"),
                (delete_key, f"{table_name}_delete_key_input", "_delete_key"),
                (distinct_on, f"[{table_name}_select_column!]", "distinct_on"),
                (inc, f"{table_name}_inc_input", "_inc"),
                (limit, "Int", "limit"),
                (offset, "Int", "offset"),
                (on_conflict, f"{table_name}_on_conflict", "on_conflict"),
                (order_by, f"[{table_name}_order_by!]", "order_by"),
                (pk_columns, f"{table_name}_pk_columns_input!", "pk_columns"),
                (prepend, f"{table_name}_prepend_input", "_prepend"),
                (updates, f"[{table_name}_updates!]!", "updates"),
                (where_req, f"{table_name}_bool_exp!", "where"),
                (where, f"{table_name}_bool_exp", "where"),
                (
                    (None, None, None)
                    if args is None
                    else (args[0], f"{args[1]}_args!", "args")
                ),
            ],
        )

        for arg_value, outer_arg_type, inner_arg_name in fragments_by_arg:
            arg_name = self._node._root._generate_var_name()
            self.variables.update({arg_name: arg_value})
            self.outer_args.append(f"${arg_name}: {outer_arg_type}")
            self.inner_args.append(f"{inner_arg_name}: ${arg_name}")

        return self

    def build_for_count(self, count: Union[bool, CountDict, None]):
        count_args_str = None
        if count is True:
            count_args_str = ""
        elif isinstance(count, dict):
            count_args: list[str] = []
            assert self._node._root
            if "columns" in count:
                table_name = self._node._get_table_name_by_model()
                var = self._node._root._generate_var_name()
                self.outer_args.append(f"${var}: [{table_name}_select_column!]")
                self.variables.update({var: count["columns"]})
                count_args.append(f"columns: ${var}")
            if "distinct" in count:
                var = self._node._root._generate_var_name()
                self.outer_args.append(f"${var}: Boolean")
                self.variables.update({var: count["distinct"]})
                count_args.append(f"distinct: ${var}")
            count_args_str = f"({', '.join(count_args)})"
        else:
            raise HasuraClientError(f"Illegal format of `count` argument: {count}")
        return count_args_str


TRECURSE = TypeVar("TRECURSE")
"""Return type of the `fn` function argument of the `BinaryTreeNode._recurse` method"""


class BinaryTreeNode(Generic[TMODEL]):
    """Represents a node in a binary tree.

    A node has one `_parent` reference and a list of `_children` references to other
    nodes in the tree. For convenience, it also has a reference to the `_root` node for
    easy access. The root node is identified by having the `_parent` as well as the
    `_root` referencing `self` (see `is_root`).
    The class comes with some convenience methods for working with the tree:

    - `_get_table_name_by_model()`: get the DB table name of the model.
    - `_bind_to_parent(parent)`: bind the node to another by setting the `_parent` and
        `_children` references in both nodes accordingly.
    - `_recurse(function)`: apply a function to each child node and append the result of
        each call to a resulting list of results.
    """

    def __init__(
        self,
        model: Type[TMODEL],
        parent: Optional[BinaryTreeNode] = None,
    ):
        super().__init__()
        self.model: Type[TMODEL] = model
        self._root: RootNode
        self._parent: BinaryTreeNode[TableModel]
        self._children: list[BinaryTreeNode[TableModel]] = []
        self._fragments = GraphQLFragments[TMODEL](self)

        if parent:
            self._bind_to_parent(parent)
            self._table_name = self._get_table_name_by_model()
            self._query_alias = self._root._generate_var_name()
        else:
            self._root = self._parent = self  # make self a root node

    def __str__(self):
        inner_args = self._fragments.inner_args
        return f"""
            {self._fragments.query_name}{
                in_brackets(', '.join(inner_args), condition=bool(inner_args))
            } {{
                {" ".join(str(rk) for rk in self._fragments.response_keys)}
            }}
        """

    @property
    def _is_root(self):
        return self._parent == self._root == self

    def _get_schema_name(self):
        return Path(inspect.getfile(self.model)).parent.name

    def _get_table_name_by_model(self):
        """
        Get the table schema and name in the format `<schema>_<table>`. The
        schema name is taken from the parent directory name the model file is
        placed in. The table name is taken from the ``_table_name`` attribute of
        the `HasuraTableModel`.
        Example:

        ```
        # models/anomaly/signal.py
        class Signal(HasuraTableModel):
            _table_name = "signals"
            uuid: UUID
            # more signal properties

        assert BinaryTreeNode(Signal)._get_table_name_by_model() == "anomaly_signals"
        ```
        """
        return self._prepend_with_schema(self.model._table_name)

    def _prepend_with_schema(self, label: str):
        schema = self._get_schema_name()
        if schema == "public":
            return label
        else:
            return f"{schema}_{label}"

    def _bind_to_parent(self, parent: BinaryTreeNode):
        self._parent = parent
        self._root = parent._root
        parent._children.append(self)
        return self

    def _recurse(self, fn: Callable[[BinaryTreeNode], TRECURSE]):
        results: list[TRECURSE] = []
        for child in self._children:
            results.append(fn(child))
            results += child._recurse(fn)
        return results
