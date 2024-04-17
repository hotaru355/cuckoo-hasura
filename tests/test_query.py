import asyncio
import logging
from datetime import datetime
from logging import LogRecord
from typing import Any, Callable, Iterable, cast
from uuid import UUID, uuid4

from httpx import AsyncClient, Client
from pytest import LogCaptureFixture, fixture, mark, raises

from cuckoo import TCOLUMNS, Insert, Query
from cuckoo.errors import HasuraClientError, HasuraServerError, RecordNotFoundError
from cuckoo.include import Include
from cuckoo.models import Aggregate, AggregateResponse
from tests.fixture.common_fixture import (
    ARTICLE_COMMENT_CONDITIONALS,
    AUTHOR_RELATIONS,
    FinalizeAggregate,
    FinalizeParams,
    FinalizeReturning,
    FinalizeWithNodes,
)
from tests.fixture.common_utils import (
    DEFAULT_COUNTS,
    all_columns,
    assert_authors_ordered,
    delete_all,
    persist_authors,
)
from tests.fixture.query_fixture import (
    AUTHOR_AGGREGATES,
    AUTHOR_ARTICLE_COMMENT_CONDITIONALS,
    AUTHOR_CONDITIONALS,
    SUGAR_FUNCTIONS,
)
from tests.fixture.sample_models.public import (
    Article,
    Author,
    AuthorBase,
    AuthorDetail,
    AuthorNumerics,
    Comment,
)


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_one())
class TestOneByPK:
    async def test_finding_a_model_if_record_exists(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        existing_uuid = cast(UUID, persisted_authors[0].uuid)

        author = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=existing_uuid),
            session=session,
            session_async=session_async,
        )

        assert isinstance(author, Author)
        assert author.uuid == existing_uuid

    async def test_not_found_error_is_raised_if_record_does_not_exist(
        self,
        finalize: FinalizeReturning[Query, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        non_existend_uuid = uuid4()

        with raises(RecordNotFoundError):
            await finalize(
                run_test=lambda Query: Query(Author).one_by_pk(uuid=non_existend_uuid),
                session=session,
                session_async=session_async,
            )

    @mark.parametrize(**ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        get_article_conditional: Callable[[Author], dict[str, Any]],
        get_comment_conditional: Callable[[Author], dict[str, Any]],
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        some_author = persisted_authors[7]
        existing_uuid = cast(UUID, some_author.uuid)
        assert existing_uuid
        expected_author = get_expected_author(some_author)

        actual_author = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=existing_uuid),
            columns=all_columns(
                article_args=get_article_conditional(some_author),
                comment_args=get_comment_conditional(some_author),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered([actual_author], [expected_author])

    @mark.parametrize(
        argnames=["get_columns", "invert_selection", "get_expected_author"],
        argvalues=[
            (
                lambda: None,
                False,
                lambda author: author.copy(include={"uuid"}),
            ),
            (
                lambda: ["name"],
                False,
                lambda author: author.copy(include={"name"}),
            ),
            (
                lambda: ["name"],
                True,
                lambda author: author.copy(exclude={"name", *AUTHOR_RELATIONS}),
            ),
            (
                lambda: [],
                True,
                lambda author: author.copy(exclude=AUTHOR_RELATIONS),
            ),
            (
                lambda: None,
                True,
                lambda author: author.copy(exclude=AUTHOR_RELATIONS),
            ),
            (
                lambda: [Include(AuthorDetail).one().returning(invert_selection=True)],
                False,
                lambda author: author.copy(include={"detail"}),
            ),
        ],
        ids=[
            "default column",
            "one column",
            "all but one column",
            "all columns, empty selection",
            "all columns, no selection",
            "all columns of included relation",
        ],
    )
    async def test_returning_columns(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        get_columns: Callable[[], TCOLUMNS],
        invert_selection: bool,
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        some_author = persisted_authors[7]
        expected = get_expected_author(some_author)

        actual = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=some_author.uuid),
            columns=get_columns(),
            invert_selection=invert_selection,
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered([actual], [expected])

    @mark.parametrize(
        argnames=["columns"],
        argvalues=[
            (["non_existing_column"],),
            ([Include(Article).many().returning()],),
            (["articles { uuid }"],),
            ([None],),
        ],
        ids=[
            "non-existing column",
            "Include",
            "relation as a string",
            "None",
        ],
    )
    async def test_raises_error_if_column_selection_is_inverted_with_invalid_columns(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        columns: TCOLUMNS,
        session: Client,
        session_async: AsyncClient,
    ):
        some_author = persisted_authors[3]

        with raises(HasuraClientError) as err:
            await finalize(
                run_test=lambda Query: Query(Author).one_by_pk(uuid=some_author.uuid),
                columns=columns,
                invert_selection=True,
                session=session,
                session_async=session_async,
            )

        assert "Invalid columns used with `invert_selection` option: " in str(err.value)

    async def test_successful_query_gets_logged(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        caplog: LogCaptureFixture,
        session: Client,
        session_async: AsyncClient,
    ):
        existing_uuid = cast(UUID, persisted_authors[0].uuid)
        logger = logging.getLogger("test_logging")
        logger.setLevel(logging.DEBUG)

        with caplog.at_level(logging.DEBUG):
            await finalize(
                run_test=lambda Query: Query(Author).one_by_pk(uuid=existing_uuid),
                columns=["name"],
                logger=logger,
                session=session,
                session_async=session_async,
            )

        record: LogRecord = next(
            filter(
                lambda rec: (
                    rec.name == "test_logging"
                    and "Query successful." in rec.msg
                    and rec.levelname == "DEBUG"
                ),
                caplog.records,
            )
        )
        assert record, "No debug log found containing 'Query successful.'"
        assert "authors_by_pk" in record.msg, "Log does not contain query name."
        assert str(existing_uuid) in record.msg, "Log does not contain query variable."
        assert (
            f'{{"name":"{persisted_authors[0].name}"}}' in record.msg
        ), "Log does not contain query result."

    async def test_failed_query_gets_logged(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        caplog: LogCaptureFixture,
        session: Client,
        session_async: AsyncClient,
    ):
        existing_uuid = cast(UUID, persisted_authors[0].uuid)
        logger = logging.getLogger("test_logging")
        logger.setLevel(logging.DEBUG)

        with raises(HasuraServerError):
            with caplog.at_level(logging.DEBUG):
                await finalize(
                    run_test=lambda Query: Query(Author).one_by_pk(uuid=existing_uuid),
                    columns=["does_not_exist"],
                    logger=logger,
                    session=session,
                    session_async=session_async,
                )

        record = next(
            filter(
                lambda rec: (
                    rec.name == "test_logging"
                    and "Query failed." in rec.msg
                    and rec.levelname == "ERROR"
                ),
                caplog.records,
            )
        )
        assert record, "No error log found containing 'Query failed.'"
        assert "authors_by_pk" in record.msg, "Log does not contain query name."
        assert str(existing_uuid) in record.msg, "Log does not contain query variable."
        assert (
            "field 'does_not_exist' not found in type: 'authors'" in record.msg
        ), "Log does not contain query error details."


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_many())
class TestMany:
    async def test_finding_all_records_with_default_column_if_no_condition_is_provided(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many(),
            session=session,
            session_async=session_async,
        )

        assert set(actual_author.uuid for actual_author in actual_authors) == set(
            persisted_author.uuid for persisted_author in persisted_authors
        )

    async def test_finding_all_records_if_no_condition_is_provided(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many(),
            columns=["name"],
            session=session,
            session_async=session_async,
        )

        assert set(actual_author.name for actual_author in actual_authors) == set(
            persisted_author.name for persisted_author in persisted_authors
        )

    async def test_returning_an_empty_list_if_non_matching_condition_is_provided(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many(
                where={"name": {"_eq": "non existing"}}
            ),
            session=session,
            session_async=session_async,
        )

        assert isinstance(actual_authors, list)
        assert len(actual_authors) == 0

    @mark.parametrize(**AUTHOR_ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        get_author_condition: Callable[[list[Author]], dict[str, Any]],
        get_article_conditional: Callable[[list[Author]], dict[str, Any]],
        get_comment_conditional: Callable[[list[Author]], dict[str, Any]],
        get_expected_authors: Callable[[list[Author]], list[Author]],
        assert_authors: Callable[[list[Author], list[Author]], None],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_authors = get_expected_authors(persisted_authors)

        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many(
                **get_author_condition(persisted_authors)
            ),
            columns=all_columns(
                article_args=get_article_conditional(persisted_authors),
                comment_args=get_comment_conditional(persisted_authors),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors(actual_authors, expected_authors)


@mark.asyncio(scope="session")
class TestManyYielding:
    @mark.performance
    @mark.slow
    async def test_memory_consumption_is_below_threshold(
        self,
        session: Client,
        session_async: AsyncClient,
    ):
        BATCH_SIZE = 10000
        CONCURRENCY = 4
        for _ in range(100):
            await asyncio.gather(
                *(
                    [
                        Insert(Author, session_async=session_async)
                        .many(
                            data=(
                                [
                                    {
                                        "name": "one of a million",
                                        "created_by": uuid4(),
                                        "updated_by": uuid4(),
                                    }
                                ]
                                * BATCH_SIZE
                            )
                        )
                        .returning_async()
                    ]
                    * CONCURRENCY
                )
            )

        print("START ", datetime.now().isoformat())
        count = 0
        # for author in await Query(Author).many(where={}).returning_async():
        for author in Query(Author).many(where={}).yielding():
            # assert isinstance(author.uuid, UUID)
            count += 1
        print("END ", count, datetime.now().isoformat())


@mark.asyncio(scope="session")
class TestAggregate:
    @mark.parametrize(**FinalizeParams(Query).aggregate())
    @mark.parametrize(**AUTHOR_AGGREGATES)
    async def test_aggregate_on_without_conditions(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        aggregate_arg: dict,
        get_value: Callable[[Aggregate], float],
        expected: float,
        session: Client,
        session_async: AsyncClient,
    ):
        aggregate_obj = await finalize(
            run_test=lambda Query: Query(
                Author,
                base_model=AuthorBase,
                numeric_model=AuthorNumerics,
            ).aggregate(),
            aggregate_args=aggregate_arg,
            session=session,
            session_async=session_async,
        )
        actual = get_value(aggregate_obj)

        assert actual == expected

    @mark.parametrize(**FinalizeParams(Query).aggregate())
    async def test_aggregate_on_raises_error_if_no_aggregate_is_provided(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(ValueError) as err:
            await finalize(
                run_test=lambda Query: Query(
                    Author,
                    base_model=AuthorBase,
                    numeric_model=AuthorNumerics,
                ).aggregate(),
                aggregate_args={},
                session=session,
                session_async=session_async,
            )

        assert (
            "Missing argument. At least one argument is required: count, avg, max, "
            "min, stddev, stddev_pop, stddev_samp, sum, var_pop, var_samp, variance."
        ) in str(err)

    @mark.parametrize(**FinalizeParams(Query).aggregate())
    @mark.parametrize(**AUTHOR_CONDITIONALS)
    async def test_aggregate_count_with_conditions(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        get_author_conditional: Callable[[list[Author]], dict],
        get_expected_authors: Callable[[list[Author]], Iterable[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_count = len(list(get_expected_authors(persisted_authors)))

        actual_count = (
            await finalize(
                run_test=lambda Query: Query(
                    Author,
                    base_model=AuthorBase,
                    numeric_model=AuthorNumerics,
                ).aggregate(**get_author_conditional(persisted_authors)),
                aggregate_args={"count": True},
                session=session,
                session_async=session_async,
            )
        ).count

        assert actual_count == expected_count

    @mark.parametrize(**FinalizeParams(Query).with_nodes())
    @mark.parametrize(**AUTHOR_CONDITIONALS)
    async def test_aggregate_count_with_nodes(
        self,
        finalize: FinalizeWithNodes[Author],
        get_author_conditional: Callable[[list[Author]], dict],
        get_expected_authors: Callable[[list[Author]], list[Author]],
        persisted_authors_with_counts: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_nodes = await finalize(
            run_test=lambda Query: Query(
                Author,
                base_model=AuthorBase,
                numeric_model=AuthorNumerics,
            ).aggregate(**get_author_conditional(persisted_authors_with_counts)),
            aggregate_args={"count": True},
            columns=all_columns(
                article_args={},
                comment_args={},
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered(
            actual_nodes, get_expected_authors(persisted_authors_with_counts)
        )

    @mark.parametrize(**SUGAR_FUNCTIONS)
    async def test_syntactic_sugar_functions(
        self,
        fn_name: str,
        args: dict,
        get_value: Callable[[Aggregate], float],
        expected: float,
        session: Client,
        session_async: AsyncClient,
    ):
        actual = getattr(
            Query(Author, session=session, session_async=session_async).aggregate(),
            fn_name,
        )(**args)

        assert get_value(actual) == expected


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_one())
class TestOneFunction:
    async def test_finding_one_record_with_default_arg_matching_record(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected = persisted_authors[0]

        actual = await finalize(
            run_test=lambda Query: Query(Author).one_function(
                "find_most_commented_author",
            ),
            session=session,
            session_async=session_async,
        )

        assert actual.uuid == expected.uuid

    async def test_finding_one_record_with_provided_arg_matching_record(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected = persisted_authors[0]

        actual = await finalize(
            run_test=lambda Query: Query(Author).one_function(
                "find_most_commented_author",
                args={"author_uuids": [author.uuid for author in persisted_authors]},
            ),
            session=session,
            session_async=session_async,
        )

        assert actual.uuid == expected.uuid

    async def test_finding_an_empty_record_with_provided_arg_not_matching_record(
        self,
        finalize: FinalizeReturning[Query, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        non_existend_uuid = uuid4()
        expected = None

        actual = await finalize(
            run_test=lambda Query: Query(Author).one_function(
                "find_most_commented_author",
                args={"author_uuids": [non_existend_uuid]},
            ),
            session=session,
            session_async=session_async,
        )

        assert actual.uuid == expected

    @mark.parametrize(**ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_authors: list[Author],
        get_article_conditional: Callable[[Author], dict[str, Any]],
        get_comment_conditional: Callable[[Author], dict[str, Any]],
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        some_author = persisted_authors[7]
        expected_author = get_expected_author(some_author)

        actual_author = await finalize(
            run_test=lambda Query: Query(Author).one_function(
                "find_most_commented_author",
                args={"author_uuids": [some_author.uuid]},
            ),
            columns=all_columns(
                article_args=get_article_conditional(some_author),
                comment_args=get_comment_conditional(some_author),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered([actual_author], [expected_author])


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_many())
class TestManyFunction:
    async def test_finding_all_records_with_default_arg_matching_records(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many_function(
                "find_authors_with_articles",
            ),
            columns=["name"],
            session=session,
            session_async=session_async,
        )

        assert set(actual_author.name for actual_author in actual_authors) == set(
            persisted_author.name for persisted_author in persisted_authors
        )

    async def test_finding_all_records_with_provided_arg_matching_records(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many_function(
                "find_authors_with_articles",
                args={"min_article_count": DEFAULT_COUNTS[Article]},
            ),
            session=session,
            session_async=session_async,
        )

        assert set(actual_author.uuid for actual_author in actual_authors) == set(
            persisted_author.uuid for persisted_author in persisted_authors
        )

    async def test_returning_an_empty_list_with_provided_arg_not_matching_record(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many_function(
                "find_authors_with_articles",
                # should return an empty list, since we are above persisted counts:
                args={"min_article_count": DEFAULT_COUNTS[Article] + 1},
            ),
            session=session,
            session_async=session_async,
        )

        assert isinstance(actual_authors, list)
        assert len(actual_authors) == 0

    @mark.parametrize(**AUTHOR_ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_columns_and_relations(
        self,
        finalize: FinalizeReturning[Query, list[Author]],
        persisted_authors: list[Author],
        get_author_condition: Callable[[list[Author]], dict[str, Any]],
        get_article_conditional: Callable[[list[Author]], dict[str, Any]],
        get_comment_conditional: Callable[[list[Author]], dict[str, Any]],
        get_expected_authors: Callable[[list[Author]], list[Author]],
        assert_authors: Callable[[list[Author], list[Author]], None],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_authors = get_expected_authors(persisted_authors)

        actual_authors = await finalize(
            run_test=lambda Query: Query(Author).many_function(
                "find_authors_with_articles",
                args={"min_article_count": DEFAULT_COUNTS[Article]},
                **get_author_condition(persisted_authors),
            ),
            columns=all_columns(
                article_args=get_article_conditional(persisted_authors),
                comment_args=get_comment_conditional(persisted_authors),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors(actual_authors, expected_authors)


@mark.asyncio(scope="session")
class TestAggregateFunction:
    @mark.parametrize(**FinalizeParams(Query).aggregate())
    @mark.parametrize(**AUTHOR_AGGREGATES)
    async def test_aggregate_on_without_conditions(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        aggregate_arg: dict,
        get_value: Callable[[Aggregate], float],
        expected: float,
        session: Client,
        session_async: AsyncClient,
    ):
        aggregate_obj = await finalize(
            run_test=lambda Query: Query(
                Author,
                base_model=AuthorBase,
                numeric_model=AuthorNumerics,
            ).aggregate_function(
                "find_authors_with_articles",
            ),
            aggregate_args=aggregate_arg,
            session=session,
            session_async=session_async,
        )
        actual = get_value(aggregate_obj)

        assert actual == expected

    @mark.parametrize(**FinalizeParams(Query).aggregate())
    async def test_aggregate_on_raises_error_if_no_aggregate_is_provided(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(ValueError) as err:
            await finalize(
                run_test=lambda Query: Query(
                    Author,
                    base_model=AuthorBase,
                    numeric_model=AuthorNumerics,
                ).aggregate_function(
                    "find_authors_with_articles",
                ),
                aggregate_args={},
                session=session,
                session_async=session_async,
            )

        assert (
            "Missing argument. At least one argument is required: count, avg, max, "
            "min, stddev, stddev_pop, stddev_samp, sum, var_pop, var_samp, variance."
        ) in str(err)

    @mark.parametrize(**FinalizeParams(Query).aggregate())
    @mark.parametrize(**AUTHOR_CONDITIONALS)
    async def test_aggregate_count_with_conditions(
        self,
        finalize: FinalizeAggregate[AuthorBase, AuthorNumerics, Author],
        get_author_conditional: Callable[[list[Author]], dict],
        get_expected_authors: Callable[[list[Author]], Iterable[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_count = len(list(get_expected_authors(persisted_authors)))

        actual_count = (
            await finalize(
                run_test=lambda Query: Query(
                    Author,
                    base_model=AuthorBase,
                    numeric_model=AuthorNumerics,
                ).aggregate_function(
                    "find_authors_with_articles",
                    **get_author_conditional(persisted_authors),
                ),
                aggregate_args={"count": True},
                session=session,
                session_async=session_async,
            )
        ).count

        assert actual_count == expected_count

    @mark.parametrize(**FinalizeParams(Query).with_nodes())
    @mark.parametrize(**AUTHOR_CONDITIONALS)
    async def test_aggregate_count_with_nodes(
        self,
        finalize: FinalizeWithNodes[Author],
        get_author_conditional: Callable[[list[Author]], dict],
        get_expected_authors: Callable[[list[Author]], list[Author]],
        persisted_authors_with_counts: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_nodes = await finalize(
            run_test=lambda Query: Query(
                Author,
                base_model=AuthorBase,
                numeric_model=AuthorNumerics,
            ).aggregate_function(
                "find_authors_with_articles",
                **get_author_conditional(persisted_authors_with_counts),
            ),
            aggregate_args={"count": True},
            columns=all_columns(
                article_args={},
                comment_args={},
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered(
            actual_nodes, get_expected_authors(persisted_authors_with_counts)
        )

    @mark.parametrize(**SUGAR_FUNCTIONS)
    async def test_syntactic_sugar_functions(
        self,
        fn_name: str,
        args: dict,
        get_value: Callable[[Aggregate], float],
        expected: float,
        session: Client,
        session_async: AsyncClient,
    ):
        actual = getattr(
            Query(
                Author, session=session, session_async=session_async
            ).aggregate_function(
                "find_authors_with_articles",
            ),
            fn_name,
        )(**args)

        assert get_value(actual) == expected


@mark.asyncio(scope="session")
class TestBatch:
    async def test_mixing_multiple_queries(
        self,
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        non_existing_uuid = uuid4()
        exisiting_uuid = persisted_authors[0].uuid

        with Query.batch(session=session) as BatchQuery:
            author1 = BatchQuery(Author).one_by_pk(uuid=exisiting_uuid).yielding()
            author2 = BatchQuery(Author).one_by_pk(uuid=non_existing_uuid).yielding()
            authors1 = (
                BatchQuery(Author)
                .many(where={"uuid": {"_eq": exisiting_uuid}})
                .yielding()
            )
            authors2 = (
                BatchQuery(Author)
                .many(where={"uuid": {"_eq": non_existing_uuid}})
                .yielding()
            )
            aggr = BatchQuery(Author).aggregate().yield_on(count=True)

        async with Query.batch_async(session_async=session_async) as BatchQuery:
            author1_async = BatchQuery(Author).one_by_pk(uuid=exisiting_uuid).yielding()
            author2_async = (
                BatchQuery(Author).one_by_pk(uuid=non_existing_uuid).yielding()
            )
            authors1_async = (
                BatchQuery(Author)
                .many(where={"uuid": {"_eq": exisiting_uuid}})
                .yielding()
            )
            authors2_async = (
                BatchQuery(Author)
                .many(where={"uuid": {"_eq": non_existing_uuid}})
                .yielding()
            )
            aggr_async = BatchQuery(Author).aggregate().yield_on(count=True)

        assert next(author1).uuid == exisiting_uuid
        assert next(author1_async).uuid == exisiting_uuid

        with raises(RecordNotFoundError):
            next(author2)
        with raises(RecordNotFoundError):
            next(author2_async)

        assert next(authors1).uuid == exisiting_uuid
        assert next(authors1_async).uuid == exisiting_uuid
        assert list(authors2) == []
        assert list(authors2_async) == []

        assert next(aggr).count == len(persisted_authors)
        assert next(aggr_async).count == len(persisted_authors)

    async def test_only_yielding_finalizers_are_returned(
        self,
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        # the queries will fail, as they are not complete
        with raises(HasuraServerError):
            with Query.batch(
                session=session,
            ) as BatchQuery:
                with raises(AttributeError):
                    (
                        BatchQuery(Author)
                        .one_by_pk(uuid=persisted_authors[0].uuid)
                        .returning(),
                    )

                with raises(AttributeError):
                    BatchQuery(Author).many().returning()

                with raises(AttributeError):
                    BatchQuery(Author).aggregate().on()

                with raises(AttributeError):
                    BatchQuery(Author).aggregate().with_nodes()

        with raises(HasuraServerError):
            async with Query.batch_async(session_async=session_async) as BatchQuery:
                with raises(AttributeError):
                    (
                        BatchQuery(Author)
                        .one_by_pk(uuid=persisted_authors[0].uuid)
                        .returning(),
                    )

                with raises(AttributeError):
                    BatchQuery(Author).many().returning()

                with raises(AttributeError):
                    BatchQuery(Author).aggregate().on()

                with raises(AttributeError):
                    BatchQuery(Author).aggregate().with_nodes()


@fixture(scope="module")
def persisted_authors(user_uuid: UUID, session: Client, session_async: AsyncClient):
    delete_all(session=session)

    return persist_authors(user_uuid, session=session, session_async=session_async)


@fixture(scope="module")
def persisted_authors_with_counts(persisted_authors: list[Author]):
    return [
        author.copy(
            update={
                "articles": [
                    article.copy(
                        update={
                            "comments": [
                                comment.copy()
                                for comment in (
                                    article.comments if article.comments else []
                                )
                            ],
                            "comments_aggregate": AggregateResponse(
                                aggregate={"count": DEFAULT_COUNTS[Comment]}
                            ),
                        }
                    )
                    for article in (author.articles if author.articles else [])
                ],
                "articles_aggregate": AggregateResponse(
                    aggregate={"count": DEFAULT_COUNTS[Article]}
                ),
            }
        )
        for author in persisted_authors
    ]
