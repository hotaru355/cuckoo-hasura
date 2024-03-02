from logging import Logger
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    Optional,
    Protocol,
    Sequence,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

from httpx import AsyncClient, Client

from cuckoo import Delete, Insert, Query, Update
from cuckoo.finalizers import (
    TRETURN,
    AffectedRowsFinalizer,
    AggregateFinalizer,
    AggregatesDict,
    ExecutingFinalizer,
    ReturningFinalizer,
    YieldingAffectedRowsFinalizer,
    YieldingAggregateFinalizer,
    YieldingFinalizer,
)
from cuckoo.include import TCOLUMNS
from cuckoo.models import (
    TMODEL,
    TMODEL_BASE,
    TNUM_PROPS,
    Aggregate,
    AggregateResponse,
)
from cuckoo.mutation import Mutation

TBUILDER = TypeVar(
    "TBUILDER",
    Query,
    Insert,
    Update,
    Delete,
    Mutation,
    covariant=True,
)


class ParameterizeArgs(TypedDict):
    argnames: Union[str, Sequence[str]]
    argvalues: Iterable[Union[Sequence[object], object]]
    ids: Optional[
        Union[
            Iterable[Union[None, str, float, int, bool]],
            Callable[[Any], Optional[object]],
        ]
    ]


class FinalizeReturning(Protocol, Generic[TBUILDER, TRETURN]):
    def __call__(
        self,
        run_test: Callable[[Type[TBUILDER]], ExecutingFinalizer],
        columns: Optional[TCOLUMNS] = None,
        invert_selection: Optional[bool] = False,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
    ) -> Awaitable[TRETURN]:
        ...


class FinalizeAggregate(
    Protocol,
    Generic[TMODEL_BASE, TNUM_PROPS, TMODEL],
):
    def __call__(
        self,
        run_test: Callable[
            [Type[Query]],
            AggregateFinalizer[TMODEL_BASE, TNUM_PROPS, TMODEL],
        ],
        aggregate_args: dict,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
    ) -> Awaitable[Aggregate[TMODEL_BASE, TNUM_PROPS]]:
        ...


class FinalizeWithNodes(Protocol, Generic[TMODEL]):
    def __call__(
        self,
        run_test: Callable[[Type[Query]], AggregateFinalizer],
        aggregate_args: dict,
        columns: Optional[TCOLUMNS] = None,
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
    ) -> Awaitable[list[TMODEL]]:
        ...


class FinalizeAffectedRows(Protocol, Generic[TBUILDER, TRETURN]):
    def __call__(
        self,
        run_test: Callable[[Type[TBUILDER]], ExecutingFinalizer],
        session: Optional[Client] = None,
        session_async: Optional[AsyncClient] = None,
        logger: Optional[Logger] = None,
    ) -> Awaitable[TRETURN]:
        ...


class FinalizeParams:
    def __init__(
        self, builder: Type[Union[Query, Insert, Update, Delete, Mutation]]
    ) -> None:
        self._builder = builder

    def _to_params(self, finalizers: list[tuple[FinalizeReturning, str]]):
        argvalues, ids = list(zip(*finalizers))

        return {
            "argnames": ["finalize"],
            "ids": ids,
            "argvalues": argvalues,
        }

    def returning_one(self):
        return self._to_params(
            [
                self._returning_finalize(),
                self._returning_async_finalize(),
                self._yielding_finalize(gen_to_val=next),
                self._yielding_in_batch_finalize(gen_to_val=next),
            ]
        )

    def returning_many(self) -> ParameterizeArgs:
        many_finalizers = [
            self._returning_finalize(),
            self._returning_async_finalize(),
            self._yielding_finalize(gen_to_val=list),
            self._yielding_in_batch_finalize(gen_to_val=list),
        ]

        with_rows_finalizers = [
            self._returning_with_rows_finalize(gen_to_val=lambda tup: tup[0]),
            self._returning_with_rows_async_finalize(gen_to_val=lambda tup: tup[0]),
            self._yielding_with_rows_finalize(gen_to_val=lambda tup: list(tup[0])),
            self._yielding_with_rows_in_batch_finalize(
                gen_to_val=lambda tup: list(tup[0])
            ),
        ]

        return self._to_params(
            many_finalizers
            if (self._builder is Query) or (self._builder is Mutation)
            else (many_finalizers + with_rows_finalizers)
        )

    def returning_many_distinct(self) -> ParameterizeArgs:
        return self._to_params(
            [
                self._returning_finalize(),
                self._returning_async_finalize(),
                self._yielding_finalize(
                    gen_to_val=lambda data_list: [list(data) for data in data_list],
                ),
                self._yielding_in_batch_finalize(
                    gen_to_val=lambda data_list: [list(data) for data in data_list],
                ),
                self._returning_with_rows_finalize(
                    gen_to_val=lambda tups: [list(tup[0]) for tup in tups],
                ),
                self._returning_with_rows_async_finalize(
                    gen_to_val=lambda tups: [list(tup[0]) for tup in tups],
                ),
                self._yielding_with_rows_finalize(
                    gen_to_val=lambda tups: [list(tup[0]) for tup in tups],
                ),
                self._yielding_with_rows_in_batch_finalize(
                    gen_to_val=lambda data_list: [list(data[0]) for data in data_list],
                ),
            ]
        )

    def affected_rows(self) -> ParameterizeArgs:
        return self._to_params(
            [
                self._affected_rows_finalize(),
                self._affected_rows_async_finalize(),
                self._yield_affected_rows_finalize(gen_to_val=next),
                self._yield_affected_rows_in_batch_finalize(gen_to_val=next),
                self._returning_with_rows_finalize(gen_to_val=lambda tup: tup[1]),
                self._returning_with_rows_async_finalize(gen_to_val=lambda tup: tup[1]),
                self._yielding_with_rows_finalize(gen_to_val=lambda tup: next(tup[1])),
                self._yielding_with_rows_in_batch_finalize(
                    gen_to_val=lambda tup: next(tup[1])
                ),
            ]
        )

    def affected_rows_distinct(self) -> ParameterizeArgs:
        return self._to_params(
            [
                self._affected_rows_finalize(),
                self._affected_rows_async_finalize(),
                self._yield_affected_rows_finalize(gen_to_val=list),
                self._yield_affected_rows_in_batch_finalize(gen_to_val=list),
                self._returning_with_rows_finalize(
                    gen_to_val=lambda tups: [tup[1] for tup in tups]
                ),
                self._returning_with_rows_async_finalize(
                    gen_to_val=lambda tups: [tup[1] for tup in tups]
                ),
                self._yielding_with_rows_finalize(
                    gen_to_val=lambda tups: [tup[1] for tup in tups]
                ),
                self._yielding_with_rows_in_batch_finalize(
                    gen_to_val=lambda tups: [tup[1] for tup in tups]
                ),
            ]
        )

    def aggregate(self) -> ParameterizeArgs:
        return self._to_params(
            [
                self._on_finalize(),
                self._on_async_finalize(),
                self._yield_on_finalize(),
                self._yield_on_batch_finalize(),
                self._with_nodes_finalize(aggr_index=0),
                self._with_nodes_async_finalize(aggr_index=0),
                self._yield_with_nodes_finalize(aggr_index=0),
                self._yield_with_nodes_batch_finalize(aggr_index=0),
            ]
        )

    def with_nodes(self) -> ParameterizeArgs:
        return self._to_params(
            [
                self._with_nodes_finalize(aggr_index=1),
                self._with_nodes_async_finalize(aggr_index=1),
                self._yield_with_nodes_finalize(aggr_index=1),
                self._yield_with_nodes_batch_finalize(aggr_index=1),
            ]
        )

    def _returning_finalize(self):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], ReturningFinalizer],
            columns: Optional[TCOLUMNS] = None,
            invert_selection: Optional[bool] = False,
            **kwargs,
        ):
            finalizer = run_test(lambda model: self._builder(model=model, **kwargs))
            return finalizer.returning(
                **({"columns": columns} if columns else {}),
                invert_selection=invert_selection,
            )

        return [finalize], "returning"

    def _returning_async_finalize(self):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], ReturningFinalizer],
            columns: Optional[TCOLUMNS] = None,
            invert_selection: Optional[bool] = False,
            **kwargs,
        ):
            finalizer = run_test(lambda model: self._builder(model=model, **kwargs))
            return await finalizer.returning_async(
                **({"columns": columns} if columns else {}),
                invert_selection=invert_selection,
            )

        return [finalize], "returning_async"

    def _yielding_finalize(
        self,
        gen_to_val=list,
    ):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], YieldingFinalizer],
            columns: Optional[TCOLUMNS] = None,
            invert_selection: Optional[bool] = False,
            **kwargs,
        ):
            finalizer: YieldingFinalizer = run_test(
                lambda model: self._builder(model=model, **kwargs)
            )
            return gen_to_val(
                finalizer.yielding(
                    **({"columns": columns} if columns else {}),
                    invert_selection=invert_selection,
                )
            )

        return [finalize], "yielding"

    def _yielding_in_batch_finalize(
        self,
        gen_to_val=list,
    ):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], ReturningFinalizer],
            columns: Optional[TCOLUMNS] = None,
            invert_selection: Optional[bool] = False,
            **kwargs,
        ):
            batch_kwargs = {
                key: value for key, value in kwargs.items() if key != "session_async"
            }
            with self._builder.batch(**batch_kwargs) as BatchBuilder:
                if self._builder == Insert:
                    BatchBuilder = BatchBuilder[0]
                elif self._builder == Update:
                    BatchBuilder = BatchBuilder[1]
                elif self._builder == Delete:
                    BatchBuilder = BatchBuilder[2]
                elif self._builder == Mutation:
                    BatchBuilder = BatchBuilder[3]

                finalizer = run_test(BatchBuilder)
                result = finalizer.yielding(
                    **({"columns": columns} if columns else {}),
                    invert_selection=invert_selection,
                )

            return gen_to_val(result)

        return [finalize], "yielding in batch"

    def _returning_with_rows_finalize(self, gen_to_val=lambda tup: tup[0]):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], AffectedRowsFinalizer],
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer = run_test(lambda model: self._builder(model=model, **kwargs))
            return gen_to_val(
                finalizer.returning_with_rows(
                    **({"columns": columns} if columns else {})
                )
            )

        return [finalize], "returning_with_rows"

    def _returning_with_rows_async_finalize(self, gen_to_val=lambda tup: tup[0]):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], AffectedRowsFinalizer],
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer = run_test(lambda model: self._builder(model=model, **kwargs))
            return gen_to_val(
                await finalizer.returning_with_rows_async(
                    **({"columns": columns} if columns else {})
                )
            )

        return [finalize], "returning_with_rows_async"

    def _yielding_with_rows_finalize(
        self,
        gen_to_val=lambda tup: list(tup[0]),
    ):
        async def finalize(
            run_test: Callable[
                [Callable[[Any], TBUILDER]], YieldingAffectedRowsFinalizer
            ],
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer = run_test(lambda model: self._builder(model=model, **kwargs))
            return gen_to_val(
                finalizer.yielding_with_rows(
                    **({"columns": columns} if columns else {})
                )
            )

        return [finalize], "yielding_with_rows"

    def _yielding_with_rows_in_batch_finalize(
        self,
        gen_to_val=lambda tup: list(tup[0]),
    ):
        async def finalize(
            run_test: Callable[
                [Callable[[Any], TBUILDER]], YieldingAffectedRowsFinalizer
            ],
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            batch_kwargs = {
                key: value for key, value in kwargs.items() if key != "session_async"
            }
            with self._builder.batch(**batch_kwargs) as BatchBuilder:
                if self._builder == Insert:
                    BatchBuilder = BatchBuilder[0]
                elif self._builder == Update:
                    BatchBuilder = BatchBuilder[1]
                elif self._builder == Delete:
                    BatchBuilder = BatchBuilder[2]

                finalizer = run_test(BatchBuilder)
                result = finalizer.yielding_with_rows(
                    **({"columns": columns} if columns else {})
                )
            return gen_to_val(result)

        return [finalize], "yielding_with_rows in batch"

    def _affected_rows_finalize(self):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], AffectedRowsFinalizer],
            **kwargs,
        ):
            finalizer = run_test(
                lambda model: self._builder(model=model, **kwargs)  # pseudo constructor
            )
            return finalizer.affected_rows()

        return [finalize], "affected_rows"

    def _affected_rows_async_finalize(self):
        async def finalize(
            run_test: Callable[[Callable[[Any], TBUILDER]], AffectedRowsFinalizer],
            **kwargs,
        ):
            finalizer = run_test(
                lambda model: self._builder(model=model, **kwargs)  # pseudo constructor
            )
            return await finalizer.affected_rows_async()

        return [finalize], "affected_rows_async"

    def _yield_affected_rows_finalize(self, gen_to_val=next):
        async def finalize(
            run_test: Callable[
                [Callable[[Any], TBUILDER]], YieldingAffectedRowsFinalizer
            ],
            **kwargs,
        ):
            finalizer = run_test(
                lambda model: self._builder(model=model, **kwargs)  # pseudo constructor
            )
            return gen_to_val(finalizer.yield_affected_rows())

        return [finalize], "yield_affected_rows"

    def _yield_affected_rows_in_batch_finalize(self, gen_to_val=list):
        async def finalize(
            run_test: Callable[
                [Callable[[Any], TBUILDER]], YieldingAffectedRowsFinalizer
            ],
            **kwargs,
        ):
            batch_kwargs = {
                key: value for key, value in kwargs.items() if key != "session_async"
            }
            with self._builder.batch(**batch_kwargs) as BatchBuilder:
                if self._builder == Insert:
                    BatchBuilder = BatchBuilder[0]
                elif self._builder == Update:
                    BatchBuilder = BatchBuilder[1]
                elif self._builder == Delete:
                    BatchBuilder = BatchBuilder[2]

                finalizer = run_test(BatchBuilder)
                result = finalizer.yield_affected_rows()

            return gen_to_val(result)

        return [finalize], "yield_affected_rows in batch"

    def _on_finalize(self):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: dict,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            return finalizer.on(**aggregate_args)

        return [finalize], "on"

    def _on_async_finalize(self):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: dict,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            return await finalizer.on_async(**aggregate_args)

        return [finalize], "on_async"

    def _yield_on_finalize(self):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: dict,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            return next(finalizer.yield_on(**aggregate_args))

        return [finalize], "yield_on"

    def _yield_on_batch_finalize(self):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: dict,
            **kwargs,
        ):
            batch_kwargs = {
                key: value for key, value in kwargs.items() if key != "session_async"
            }
            with self._builder.batch(**batch_kwargs) as BatchBuilder:
                finalizer: YieldingAggregateFinalizer = run_test(
                    lambda model, base_model, numeric_model: BatchBuilder(
                        model=model,
                        base_model=base_model,
                        numeric_model=numeric_model,
                    )
                )
                result = finalizer.yield_on(**aggregate_args)

            return next(result)

        return [finalize], "yield_on in batch"

    def _with_nodes_finalize(
        self,
        aggr_index: int,
    ):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: AggregatesDict,
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            return finalizer.with_nodes(
                aggregates=aggregate_args, **({"columns": columns} if columns else {})
            )[aggr_index]

        return [finalize], "with_nodes"

    def _with_nodes_async_finalize(
        self,
        aggr_index: int,
    ):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: AggregatesDict,
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            return (
                await finalizer.with_nodes_async(
                    aggregates=aggregate_args,
                    **({"columns": columns} if columns else {}),
                )
            )[aggr_index]

        return ([finalize], "with_nodes_async")

    def _yield_with_nodes_finalize(
        self,
        aggr_index: int,
    ):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: AggregatesDict,
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            finalizer: AggregateFinalizer = run_test(
                lambda model, base_model, numeric_model: self._builder(
                    model=model,
                    base_model=base_model,
                    numeric_model=numeric_model,
                    **kwargs,
                )
            )
            gen_to_val = next if aggr_index == 0 else list

            return gen_to_val(
                finalizer.yield_with_nodes(
                    aggregates=aggregate_args,
                    **({"columns": columns} if columns else {}),
                )[aggr_index]
            )

        return ([finalize], "yield_with_nodes")

    def _yield_with_nodes_batch_finalize(
        self,
        aggr_index: int,
    ):
        async def finalize(
            run_test: Callable[[Type[TBUILDER]], AggregateFinalizer],
            aggregate_args: AggregatesDict,
            columns: Optional[TCOLUMNS] = None,
            **kwargs,
        ):
            batch_kwargs = {
                key: value for key, value in kwargs.items() if key != "session_async"
            }
            gen_to_val = next if aggr_index == 0 else list
            with self._builder.batch(**batch_kwargs) as BatchBuilder:
                finalizer: YieldingAggregateFinalizer = run_test(
                    lambda model, base_model, numeric_model: BatchBuilder(
                        model=model,
                        base_model=base_model,
                        numeric_model=numeric_model,
                    )
                )
                result = finalizer.yield_with_nodes(
                    aggregates=aggregate_args,
                    **({"columns": columns} if columns else {}),
                )[aggr_index]

            return gen_to_val(result)

        return ([finalize], "yield_with_nodes in batch")


ARTICLE_COMMENT_CONDITIONALS: ParameterizeArgs = {
    "argnames": [
        "get_article_conditional",
        "get_comment_conditional",
        "get_expected_author",
    ],
    "ids": [
        "NONE",
        "where",
        "order_by",
        "limit",
        "offset",
        "distinct_on",
        "ALL",
    ],
    "argvalues": [
        (
            lambda author: {},
            lambda author: {},
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        article.copy(
                            update={
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 5}
                                ),
                            }
                        )
                        for article in author.articles
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 5}),
                }
            ),
        ),
        (
            lambda author: {
                "where": {
                    "title": {"_eq": author.articles[3].title},
                }
            },
            lambda author: {
                "where": {"content": {"_eq": author.articles[3].comments[2].content}}
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        author.articles[3].copy(
                            update={
                                "comments": [author.articles[3].comments[2].copy()],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 1}
                                ),
                            }
                        )
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 1}),
                }
            ),
        ),
        (
            lambda author: {
                "order_by": {"title": "desc"},
            },
            lambda author: {
                "order_by": {"content": "desc"},
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        article.copy(
                            update={
                                "comments": [
                                    comment.copy()
                                    for comment in reversed(article.comments)
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 5}
                                ),
                            }
                        )
                        for article in reversed(author.articles)
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 5}),
                }
            ),
        ),
        (
            lambda author: {
                "limit": 2,
                "order_by": {"title": "desc"},
            },
            lambda author: {
                "limit": 3,
                "order_by": {"content": "desc"},
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        author.articles[4].copy(
                            update={
                                "comments": [
                                    author.articles[4].comments[4].copy(),
                                    author.articles[4].comments[3].copy(),
                                    author.articles[4].comments[2].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                        author.articles[3].copy(
                            update={
                                "comments": [
                                    author.articles[3].comments[4].copy(),
                                    author.articles[3].comments[3].copy(),
                                    author.articles[3].comments[2].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 2}),
                }
            ),
        ),
        (
            lambda author: {
                "offset": 3,
                "order_by": {"title": "desc"},
            },
            lambda author: {
                "offset": 2,
                "order_by": {"content": "desc"},
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        author.articles[1].copy(
                            update={
                                "comments": [
                                    author.articles[1].comments[2].copy(),
                                    author.articles[1].comments[1].copy(),
                                    author.articles[1].comments[0].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                        author.articles[0].copy(
                            update={
                                "comments": [
                                    author.articles[0].comments[2].copy(),
                                    author.articles[0].comments[1].copy(),
                                    author.articles[0].comments[0].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 2}),
                }
            ),
        ),
        (
            lambda author: {
                "distinct_on": "word_count",
                "order_by": [
                    {"word_count": "asc"},
                    {"title": "desc"},
                ],
            },
            lambda author: {
                "distinct_on": "likes",
                "order_by": [
                    {"likes": "desc"},
                    {"content": "desc"},
                ],
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        author.articles[3].copy(
                            update={
                                "comments": [
                                    author.articles[3].comments[2].copy(),
                                    author.articles[3].comments[4].copy(),
                                    author.articles[3].comments[3].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                        author.articles[4].copy(
                            update={
                                "comments": [
                                    author.articles[4].comments[2].copy(),
                                    author.articles[4].comments[4].copy(),
                                    author.articles[4].comments[3].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                        author.articles[2].copy(
                            update={
                                "comments": [
                                    author.articles[2].comments[2].copy(),
                                    author.articles[2].comments[4].copy(),
                                    author.articles[2].comments[3].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 3}
                                ),
                            }
                        ),
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 3}),
                }
            ),
        ),
        (
            lambda author: {
                "where": {
                    "word_count": {"_lt": 3000},
                },
                "order_by": [
                    {"word_count": "asc"},
                    {"title": "desc"},
                ],
                "distinct_on": "word_count",
                "limit": 1,
                "offset": 1,
            },
            lambda author: {
                "where": {
                    "likes": {"_lt": 3},
                },
                "order_by": [
                    {"likes": "desc"},
                    {"content": "desc"},
                ],
                "distinct_on": "likes",
                "limit": 1,
                "offset": 1,
            },
            lambda author: author.copy(
                update={
                    "name": "updated",
                    "articles": [
                        author.articles[4].copy(
                            update={
                                "comments": [
                                    author.articles[4].comments[3].copy(),
                                ],
                                "comments_aggregate": AggregateResponse(
                                    aggregate={"count": 1}
                                ),
                            }
                        ),
                    ],
                    "articles_aggregate": AggregateResponse(aggregate={"count": 1}),
                }
            ),
        ),
    ],
}
