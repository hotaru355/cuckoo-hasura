from __future__ import annotations
from typing import (
    Callable,
    Generator,
    Generic,
    Optional,
    TypeVar,
    TypedDict,
    Union,
    TYPE_CHECKING,
)
from typing_extensions import NotRequired

from cuckoo.binary_tree_node import (
    AffectedRowsResponseKey,
    AggregateResponseKey,
    BinaryTreeNode,
    ColumnResponseKey,
    NodesResponseKey,
    ReturningResponseKey,
)
from cuckoo.models.aggregate import TMODEL_BASE, TNUM_PROPS, Aggregate
from cuckoo.models.common import TMODEL

if TYPE_CHECKING:
    from cuckoo.include import TINCLUDE, TCOLUMNS


class CountDict(TypedDict):
    columns: NotRequired[set[str]]
    distinct: NotRequired[bool]


class AggregatesDict(TypedDict):
    count: Union[bool, CountDict]
    avg: set[str]
    max: set[str]
    min: set[str]
    stddev: set[str]
    stddev_pop: set[str]
    stddev_samp: set[str]
    sum: set[str]
    var_pop: set[str]
    var_samp: set[str]
    variance: set[str]


TYIELD = TypeVar("TYIELD")
TRETURN = TypeVar("TRETURN")
TYIELD_ROWS = TypeVar("TYIELD_ROWS")
TRETURN_ROWS = TypeVar("TRETURN_ROWS")
TYIELD_WITH = TypeVar("TYIELD_WITH")
TRETURN_WITH = TypeVar("TRETURN_WITH")


class Finalizer:
    def __init__(
        self,
        node: BinaryTreeNode,
    ):
        super().__init__()
        self._node: BinaryTreeNode = node


class ExecutingFinalizer(Finalizer):
    def __init__(
        self,
        node: BinaryTreeNode,
        **kwargs,
    ):
        super().__init__(node=node, **kwargs)

    @staticmethod
    def _bind_includes(node: BinaryTreeNode):
        response_key: Optional[ColumnResponseKey] = next(
            filter(
                lambda key: isinstance(key, ColumnResponseKey),
                node._fragments.response_keys,
            ),
            None,
        )
        if response_key is None:
            return

        for i, str_or_constructor in enumerate(response_key._columns):
            if isinstance(str_or_constructor, str):
                continue
            elif isinstance(str_or_constructor, Callable):
                include = str_or_constructor(node)
                response_key._columns[i] = include
                ExecutingFinalizer._bind_includes(include)
            elif isinstance(str_or_constructor, BinaryTreeNode):
                raise ValueError(
                    "A list of columns can only be used once if they include a "
                    "an instance of `Include`. Found an instance "
                    f"`Include({str_or_constructor.model.__name__})` that was already "
                    "used in an executed query."
                )
            else:
                raise ValueError(
                    "Elements in `returning` need to be of type `str` or "
                    "an instance of `Include`. "
                    f"Found type={type(str_or_constructor)}."
                )

    def _execute(self):
        if not self._node._root._is_batch:
            self._node._root._execute()

    async def _execute_async(self):
        if not self._node._root._is_batch:
            await self._node._root._execute_async()


class AggregateBaseFinalizer(Finalizer):
    def _validate_args(self, aggregates: AggregatesDict):
        if not any(aggregates.values()):
            raise ValueError(
                "Missing argument. At least one argument is required: count, avg, max, "
                "min, stddev, stddev_pop, stddev_samp, sum, var_pop, var_samp, "
                "variance."
            )

    def _to_aggr_dict(
        self,
        count: Optional[Union[bool, CountDict]] = None,
        avg: Optional[set[str]] = None,
        max: Optional[set[str]] = None,
        min: Optional[set[str]] = None,
        stddev: Optional[set[str]] = None,
        stddev_pop: Optional[set[str]] = None,
        stddev_samp: Optional[set[str]] = None,
        sum: Optional[set[str]] = None,
        var_pop: Optional[set[str]] = None,
        var_samp: Optional[set[str]] = None,
        variance: Optional[set[str]] = None,
    ) -> AggregatesDict:
        return {
            key: value
            for key, value in [
                ("count", count),
                ("avg", avg),
                ("max", max),
                ("min", min),
                ("stddev", stddev),
                ("stddev_pop", stddev_pop),
                ("stddev_samp", stddev_samp),
                ("sum", sum),
                ("var_pop", var_pop),
                ("var_samp", var_samp),
                ("variance", variance),
            ]
            if value is not None
        }

    def _build_count_fragments(self, aggr: AggregatesDict):
        return {
            key: (
                self._node._fragments.build_for_count(value)
                if key == "count"
                else value
            )
            for key, value in aggr.items()
        }


class YieldingFinalizer(
    ExecutingFinalizer,
    Generic[TYIELD],
):
    def __init__(
        self: YieldingFinalizer,
        node: BinaryTreeNode,
        returning_fn: Callable[[], Generator[TYIELD, None, None]],
        gen_to_val: dict,
        response_key=ColumnResponseKey,
        **kwargs,
    ):
        super().__init__(node=node, **kwargs)
        self._returning_fn = returning_fn
        self._response_key = response_key
        self._gen_to_val = gen_to_val

    def yielding(
        self: YieldingFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> Generator[TYIELD, None, None]:
        self._node._fragments.response_keys.append(self._response_key(columns))
        ExecutingFinalizer._bind_includes(self._node)
        self._execute()

        return self._returning_fn()


class ReturningFinalizer(
    YieldingFinalizer[TYIELD],
    Generic[TYIELD, TRETURN],
):
    def returning(
        self: ReturningFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> TRETURN:
        return self._gen_to_val["returning"](super().yielding(columns))

    async def returning_async(
        self: ReturningFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> TRETURN:
        self._node._fragments.response_keys.append(self._response_key(columns))
        ExecutingFinalizer._bind_includes(self._node)
        await self._execute_async()

        return self._gen_to_val["returning"](self._returning_fn())


class YieldingAffectedRowsFinalizer(
    YieldingFinalizer[TYIELD],
    Generic[TYIELD, TYIELD_WITH, TYIELD_ROWS],
):
    def __init__(
        self: YieldingAffectedRowsFinalizer,
        node: BinaryTreeNode,
        returning_fn: Callable[[], Generator[TYIELD, None, None]],
        affected_rows_fn: Callable[[], TYIELD_ROWS],
        returning_with_rows_fn: Callable[[], TYIELD_WITH],
        gen_to_val: dict,
    ):
        super().__init__(
            node=node,
            returning_fn=returning_fn,
            response_key=ReturningResponseKey,
            gen_to_val=gen_to_val,
        )
        self._affected_rows_fn = affected_rows_fn
        self._returning_with_rows_fn = returning_with_rows_fn

    def yield_affected_rows(
        self: YieldingAffectedRowsFinalizer,
    ) -> Generator[TYIELD_ROWS, None, None]:
        self._node._fragments.response_keys.append(AffectedRowsResponseKey())
        self._execute()

        return self._affected_rows_fn()

    def yielding_with_rows(
        self: YieldingAffectedRowsFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> TYIELD_WITH:
        self._node._fragments.response_keys.extend(
            [self._response_key(columns), AffectedRowsResponseKey()]
        )
        ExecutingFinalizer._bind_includes(self._node)
        self._execute()

        return self._returning_with_rows_fn()


class AffectedRowsFinalizer(
    ReturningFinalizer[TYIELD, TRETURN],
    YieldingAffectedRowsFinalizer[TYIELD, TYIELD_WITH, TYIELD_ROWS],
    Generic[TYIELD, TRETURN, TYIELD_WITH, TRETURN_WITH, TYIELD_ROWS, TRETURN_ROWS],
):
    def affected_rows(self: AffectedRowsFinalizer) -> TRETURN_ROWS:
        return self._gen_to_val["affected_rows"](super().yield_affected_rows())

    async def affected_rows_async(
        self: YieldingAffectedRowsFinalizer,
    ) -> TRETURN_ROWS:
        self._node._fragments.response_keys.append(AffectedRowsResponseKey())
        await self._execute_async()

        return self._gen_to_val["affected_rows"](self._affected_rows_fn())

    def returning_with_rows(
        self: AffectedRowsFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> TRETURN_WITH:
        return self._gen_to_val["returning_with_rows"](
            super().yielding_with_rows(columns=columns)
        )

    async def returning_with_rows_async(
        self: YieldingAffectedRowsFinalizer,
        columns: TCOLUMNS = ["uuid"],
    ) -> TRETURN_WITH:
        self._node._fragments.response_keys.extend(
            [self._response_key(columns), AffectedRowsResponseKey()]
        )
        ExecutingFinalizer._bind_includes(self._node)
        await self._execute_async()

        return self._gen_to_val["returning_with_rows"](self._returning_with_rows_fn())


class YieldingAggregateFinalizer(
    ExecutingFinalizer,
    AggregateBaseFinalizer,
    Generic[TMODEL_BASE, TNUM_PROPS, TMODEL],
):
    def __init__(
        self: YieldingAggregateFinalizer,
        node: BinaryTreeNode,
        aggregate_fn: Callable[
            [], Generator[Aggregate[TMODEL_BASE, TNUM_PROPS], None, None]
        ],
        nodes_fn: Callable[[], Generator[TMODEL, None, None]],
        **kwargs,
    ):
        super().__init__(
            node=node,
            **kwargs,
        )
        self._aggregate_fn = aggregate_fn
        self._nodes_fn = nodes_fn

    def yield_on(
        self: YieldingAggregateFinalizer,
        count: Optional[Union[bool, CountDict]] = None,
        avg: Optional[set[str]] = None,
        max: Optional[set[str]] = None,
        min: Optional[set[str]] = None,
        stddev: Optional[set[str]] = None,
        stddev_pop: Optional[set[str]] = None,
        stddev_samp: Optional[set[str]] = None,
        sum: Optional[set[str]] = None,
        var_pop: Optional[set[str]] = None,
        var_samp: Optional[set[str]] = None,
        variance: Optional[set[str]] = None,
    ) -> Generator[Aggregate[TMODEL_BASE, TNUM_PROPS], None, None]:
        aggregates = self._to_aggr_dict(
            count,
            avg,
            max,
            min,
            stddev,
            stddev_pop,
            stddev_samp,
            sum,
            var_pop,
            var_samp,
            variance,
        )
        self._validate_args(aggregates)
        self._node._fragments.response_keys.append(
            AggregateResponseKey(aggregates=self._build_count_fragments(aggregates))
        )
        self._execute()

        return self._aggregate_fn()

    def yield_with_nodes(
        self: YieldingAggregateFinalizer,
        aggregates: AggregatesDict,
        columns: TCOLUMNS = ["uuid"],
    ) -> tuple[Generator[Aggregate, None, None], Generator[TMODEL, None, None]]:
        self._validate_args(aggregates)
        self._node._fragments.response_keys.extend(
            [
                AggregateResponseKey(
                    aggregates=self._build_count_fragments(aggregates)
                ),
                NodesResponseKey(columns=columns),
            ]
        )
        ExecutingFinalizer._bind_includes(self._node)
        self._execute()

        return (self._aggregate_fn(), self._nodes_fn())


class AggregateFinalizer(
    YieldingAggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
    Generic[TMODEL_BASE, TNUM_PROPS, TMODEL],
):
    def on(
        self,
        count: Optional[Union[bool, CountDict]] = None,
        avg: Optional[set[str]] = None,
        max: Optional[set[str]] = None,
        min: Optional[set[str]] = None,
        stddev: Optional[set[str]] = None,
        stddev_pop: Optional[set[str]] = None,
        stddev_samp: Optional[set[str]] = None,
        sum: Optional[set[str]] = None,
        var_pop: Optional[set[str]] = None,
        var_samp: Optional[set[str]] = None,
        variance: Optional[set[str]] = None,
    ) -> Aggregate[TMODEL_BASE, TNUM_PROPS]:
        return next(
            self.yield_on(
                count=count,
                avg=avg,
                max=max,
                min=min,
                stddev=stddev,
                stddev_pop=stddev_pop,
                stddev_samp=stddev_samp,
                sum=sum,
                var_pop=var_pop,
                var_samp=var_samp,
                variance=variance,
            )
        )

    async def on_async(
        self,
        count: Optional[Union[bool, CountDict]] = None,
        avg: Optional[set[str]] = None,
        max: Optional[set[str]] = None,
        min: Optional[set[str]] = None,
        stddev: Optional[set[str]] = None,
        stddev_pop: Optional[set[str]] = None,
        stddev_samp: Optional[set[str]] = None,
        sum: Optional[set[str]] = None,
        var_pop: Optional[set[str]] = None,
        var_samp: Optional[set[str]] = None,
        variance: Optional[set[str]] = None,
    ) -> Aggregate[TMODEL_BASE, TNUM_PROPS]:
        aggregates = self._to_aggr_dict(
            count,
            avg,
            max,
            min,
            stddev,
            stddev_pop,
            stddev_samp,
            sum,
            var_pop,
            var_samp,
            variance,
        )
        self._validate_args(aggregates)
        self._node._fragments.response_keys.append(
            AggregateResponseKey(aggregates=self._build_count_fragments(aggregates))
        )
        await self._execute_async()

        return next(self._aggregate_fn())

    def with_nodes(
        self,
        aggregates: AggregatesDict,
        columns: TCOLUMNS = ["uuid"],
    ) -> tuple[Aggregate[TMODEL_BASE, TNUM_PROPS], list[TMODEL]]:
        aggregate_generator, nodes_generator = self.yield_with_nodes(
            aggregates=aggregates, columns=columns
        )

        return (next(aggregate_generator), list(nodes_generator))

    async def with_nodes_async(
        self,
        aggregates: AggregatesDict,
        columns: TCOLUMNS = ["uuid"],
    ) -> tuple[Aggregate[TMODEL_BASE, TNUM_PROPS], list[TMODEL]]:
        self._validate_args(aggregates)
        self._node._fragments.response_keys.extend(
            [
                AggregateResponseKey(
                    aggregates=self._build_count_fragments(aggregates)
                ),
                NodesResponseKey(columns=columns),
            ]
        )
        ExecutingFinalizer._bind_includes(self._node)
        await self._execute_async()

        return (next(self._aggregate_fn()), list(self._nodes_fn()))

    def count(
        self,
        columns: Optional[set[str]] = None,
        distinct: Optional[bool] = None,
    ):
        count_arg: CountDict = {}
        if columns is not None:
            count_arg.update({"columns": columns})
        if distinct is not None:
            count_arg.update({"distinct": distinct})

        return self.on(count=count_arg or True).count

    def avg(self, columns: set[str]):
        return self.on(avg=columns).avg

    def max(self, columns: set[str]):
        return self.on(max=columns).max

    def min(self, columns: set[str]):
        return self.on(min=columns).min

    def sum(self, columns: set[str]):
        return self.on(sum=columns).sum


class IncludeFinalizer(Finalizer):
    def __init__(
        self,
        node: BinaryTreeNode,
        finalize_fn: TINCLUDE,
        **kwargs,
    ):
        super().__init__(
            node=node,
            **kwargs,
        )
        self._finalize_fn = finalize_fn

    def returning(self, columns: TCOLUMNS = ["uuid"]):
        self._node._fragments.response_keys.append(ColumnResponseKey(columns))
        return self._finalize_fn


class AggregateIncludeFinalizer(AggregateBaseFinalizer):
    def __init__(
        self,
        node: BinaryTreeNode,
        finalize_fn: TINCLUDE,
        **kwargs,
    ):
        super().__init__(
            node=node,
            **kwargs,
        )
        self._finalize_fn = finalize_fn

    def on(
        self,
        count: Optional[Union[bool, CountDict]] = None,
        avg: Optional[set[str]] = None,
        max: Optional[set[str]] = None,
        min: Optional[set[str]] = None,
        stddev: Optional[set[str]] = None,
        stddev_pop: Optional[set[str]] = None,
        stddev_samp: Optional[set[str]] = None,
        sum: Optional[set[str]] = None,
        var_pop: Optional[set[str]] = None,
        var_samp: Optional[set[str]] = None,
        variance: Optional[set[str]] = None,
    ):
        aggregates = self._to_aggr_dict(
            count,
            avg,
            max,
            min,
            stddev,
            stddev_pop,
            stddev_samp,
            sum,
            var_pop,
            var_samp,
            variance,
        )
        self._validate_args(aggregates)

        def bind_and_configure(parent):
            include = self._finalize_fn(parent)
            self._node._fragments.response_keys.append(
                AggregateResponseKey(aggregates=self._build_count_fragments(aggregates))
            )
            return include

        return bind_and_configure

    def with_nodes(
        self,
        aggregates: AggregatesDict,
        columns: TCOLUMNS,
    ):
        self._node._fragments.response_keys.append(NodesResponseKey(columns=columns))

        return self.on(**aggregates)

    def count(
        self,
        columns: Optional[set[str]] = None,
        distinct: Optional[bool] = None,
    ):
        count_arg: CountDict = {}
        if columns is not None:
            count_arg.update({"columns": columns})
        if distinct is not None:
            count_arg.update({"distinct": distinct})

        return self.on(count=count_arg or True)

    def avg(self, columns: set[str]):
        return self.on(avg=columns)

    def max(self, columns: set[str]):
        return self.on(max=columns)

    def min(self, columns: set[str]):
        return self.on(min=columns)

    def sum(self, columns: set[str]):
        return self.on(sum=columns)


TFIN_ONE = TypeVar("TFIN_ONE")
TFIN_MANY = TypeVar("TFIN_MANY")
TFIN_MANY_DISTINCT = TypeVar("TFIN_MANY_DISTINCT")
TFIN_AGGR = TypeVar("TFIN_AGGR")
