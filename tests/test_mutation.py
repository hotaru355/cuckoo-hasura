import asyncio
from typing import Any, Callable, Type
from unittest.mock import ANY
from uuid import UUID, uuid4

from geojson_pydantic import Polygon
from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo import Mutation, Query
from cuckoo.delete import BatchDelete
from cuckoo.errors import HasuraClientError
from cuckoo.insert import BatchInsert
from cuckoo.update import BatchUpdate
from tests.fixture.common_fixture import (
    ARTICLE_COMMENT_CONDITIONALS,
    FinalizeParams,
    FinalizeReturning,
)
from tests.fixture.common_utils import (
    all_columns,
    assert_authors_ordered,
    delete_all,
    persist_authors,
)
from tests.fixture.mutation_fixture import MUTATIONS1, MUTATIONS2
from tests.fixture.query_fixture import (
    AUTHOR_ARTICLE_COMMENT_CONDITIONALS,
)
from tests.fixture.sample_models import Author


@mark.asyncio(scope="session")
class TestBatch:
    @mark.parametrize(**MUTATIONS1)
    @mark.parametrize(**MUTATIONS2)
    async def test_two_mutations_in_batch(
        self,
        persisted_authors: list[Author],
        run_and_assert1: Callable[
            [list[Author], Client],
            tuple[
                Callable[
                    [Type[BatchInsert], Type[BatchUpdate], Type[BatchDelete], UUID], Any
                ],
                Callable[[Any], None],
            ],
        ],
        run_and_assert2: Callable[
            [list[Author], Client],
            tuple[
                Callable[
                    [Type[BatchInsert], Type[BatchUpdate], Type[BatchDelete], UUID], Any
                ],
                Callable[[Any], None],
            ],
        ],
        user_uuid: UUID,
        session: Client,
    ):
        run_mutation1, assert_model1 = run_and_assert1(persisted_authors, session)
        run_mutation2, assert_model2 = run_and_assert2(persisted_authors, session)

        with Mutation.batch(session=session) as (
            BatchInsert,
            BatchUpdate,
            BatchDelete,
            BatchMutation,
        ):
            actual1 = run_mutation1(
                BatchInsert,
                BatchUpdate,
                BatchDelete,
                user_uuid,
            )
            actual2 = run_mutation2(
                BatchInsert,
                BatchUpdate,
                BatchDelete,
                user_uuid,
            )

        assert_model1(actual1)
        assert_model2(actual2)

    @mark.parametrize(**MUTATIONS1)
    @mark.parametrize(**MUTATIONS2)
    async def test_two_mutations_in_batch_async(
        self,
        persisted_authors: list[Author],
        run_and_assert1: Callable[
            [list[Author], Client],
            tuple[
                Callable[
                    [Type[BatchInsert], Type[BatchUpdate], Type[BatchDelete], UUID], Any
                ],
                Callable[[Any], None],
            ],
        ],
        run_and_assert2: Callable[
            [list[Author], Client],
            tuple[
                Callable[
                    [Type[BatchInsert], Type[BatchUpdate], Type[BatchDelete], UUID], Any
                ],
                Callable[[Any], None],
            ],
        ],
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        run_mutation1, assert_model1 = run_and_assert1(persisted_authors, session)
        run_mutation2, assert_model2 = run_and_assert2(persisted_authors, session)

        async with Mutation.batch_async(session_async=session_async) as (
            BatchInsert,
            BatchUpdate,
            BatchDelete,
            BatchMutation,
        ):
            actual1 = run_mutation1(
                BatchInsert,
                BatchUpdate,
                BatchDelete,
                user_uuid,
            )
            actual2 = run_mutation2(
                BatchInsert,
                BatchUpdate,
                BatchDelete,
                user_uuid,
            )

        assert_model1(actual1)
        assert_model2(actual2)

    async def test_raises_client_error_when_trying_to_access_result_of_unexecuted_batch(
        self,
        session: Client,
    ):
        with raises(HasuraClientError) as err:
            with Mutation.batch(session=session) as (_, _, BatchDelete, _):
                actual1 = BatchDelete(Author).many(where={}).yielding()

                next(actual1)

        assert (
            "Cannot access response data before calling `RootNode._make_request`"
            in str(err)
        )


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Mutation).returning_one())
class TestOneFunction:
    async def test_mutating_one_record_with_provided_arg_matching_record(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_author = persisted_authors[4]
        expected = random_author.age + 1

        actual = (
            await finalize(
                run_test=lambda Mutation: Mutation(Author).one_function(
                    "inc_author_age",
                    args={
                        "author_uuid": random_author.uuid,
                        "user_uuid": uuid4(),
                    },
                ),
                columns=["age"],
                session=session,
                session_async=session_async,
            )
        ).age

        assert actual == expected

    async def test_mutating_one_record_with_integer_argument(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_author = persisted_authors[4]
        expected = None

        actual = (
            await finalize(
                run_test=lambda Mutation: Mutation(Author).one_function(
                    "inc_author_age",
                    args={
                        "author_uuid": random_author.uuid,
                        "user_uuid": uuid4(),
                        "only_if_older_than": 100,
                    },
                ),
                columns=["age"],
                session=session,
                session_async=session_async,
            )
        ).age

        assert actual == expected

    async def test_mutating_one_record_with_boolean_argument(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_author = persisted_authors[4]
        expected = None

        actual = (
            await finalize(
                run_test=lambda Mutation: Mutation(Author).one_function(
                    "inc_author_age",
                    args={
                        "author_uuid": random_author.uuid,
                        "user_uuid": uuid4(),
                        "has_articles": False,
                    },
                ),
                columns=["age"],
                session=session,
                session_async=session_async,
            )
        ).age

        assert actual == expected

    async def test_mutating_one_record_with_geometry_argument(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_author = persisted_authors[4]
        updated_home_zone = Polygon(
            type="Polygon",
            coordinates=[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]],
        )
        expected = updated_home_zone.dict(exclude_none=True)

        actual = (
            await finalize(
                run_test=lambda Mutation: Mutation(Author).one_function(
                    "inc_author_age",
                    args={
                        "author_uuid": random_author.uuid,
                        "user_uuid": uuid4(),
                        "updated_home_zone": updated_home_zone,
                    },
                ),
                columns=["home_zone"],
                session=session,
                session_async=session_async,
            )
        ).home_zone.dict(exclude_none=True)

        assert actual == expected

    async def test_returning_empty_record_with_provided_arg_not_matching_record(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_uuid = uuid4()
        expected = None

        actual = (
            await finalize(
                run_test=lambda Mutation: Mutation(Author).one_function(
                    "inc_author_age",
                    args={
                        "author_uuid": random_uuid,
                        "user_uuid": uuid4(),
                    },
                ),
                columns=["age"],
                session=session,
                session_async=session_async,
            )
        ).age

        assert actual == expected

    @mark.parametrize(**ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Mutation, Author],
        persisted_authors: list[Author],
        get_article_conditional: Callable[[Author], dict[str, Any]],
        get_comment_conditional: Callable[[Author], dict[str, Any]],
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_author = persisted_authors[7]
        user_uuid = uuid4()
        expected_author = get_expected_author(random_author)
        expected_author.age += 1
        expected_author.updated_by = user_uuid
        expected_author.updated_at = ANY

        actual_author = await finalize(
            run_test=lambda Mutation: Mutation(Author).one_function(
                "inc_author_age",
                args={
                    "author_uuid": random_author.uuid,
                    "user_uuid": user_uuid,
                },
            ),
            columns=all_columns(
                article_args=get_article_conditional(random_author),
                comment_args=get_comment_conditional(random_author),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered([actual_author], [expected_author])


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Mutation).returning_many())
class TestManyFunction:
    async def test_mutating_records_with_provided_arg_matching_records(
        self,
        finalize: FinalizeReturning[Mutation, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        random_authors = [
            persisted_authors[3],
            persisted_authors[6],
            persisted_authors[8],
        ]
        actual_authors = await finalize(
            run_test=lambda Mutation: Mutation(Author).many_function(
                "inc_all_authors_age",
                args={
                    "author_uuids": [author.uuid for author in random_authors],
                    "user_uuid": uuid4(),
                },
            ),
            session=session,
            session_async=session_async,
        )

        assert set(actual_author.uuid for actual_author in actual_authors) == set(
            author.uuid for author in random_authors
        )

        for updated_author, random_author in zip(
            await asyncio.gather(
                *[
                    Query(Author, session_async=session_async)
                    .one_by_pk(uuid=author.uuid)
                    .returning_async(["age"])
                    for author in random_authors
                ]
            ),
            random_authors,
        ):
            assert updated_author.age == random_author.age + 1

    async def test_returning_an_empty_list_with_provided_arg_not_matching_record(
        self,
        finalize: FinalizeReturning[Mutation, list[Author]],
        session: Client,
        session_async: AsyncClient,
    ):
        random_uuids = [uuid4(), uuid4()]

        actual_authors = await finalize(
            run_test=lambda Mutation: Mutation(Author).many_function(
                "inc_all_authors_age",
                args={
                    "author_uuids": random_uuids,
                    "user_uuid": uuid4(),
                },
            ),
            session=session,
            session_async=session_async,
        )

        assert isinstance(actual_authors, list)
        assert len(actual_authors) == 0

    @mark.parametrize(**AUTHOR_ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_columns_and_relations(
        self,
        finalize: FinalizeReturning[Mutation, list[Author]],
        persisted_authors: list[Author],
        get_author_condition: Callable[[list[Author]], dict[str, Any]],
        get_article_conditional: Callable[[list[Author]], dict[str, Any]],
        get_comment_conditional: Callable[[list[Author]], dict[str, Any]],
        get_expected_authors: Callable[[list[Author]], list[Author]],
        assert_authors: Callable[[list[Author], list[Author]], None],
        session: Client,
        session_async: AsyncClient,
    ):
        user_uuid = uuid4()
        expected_authors = [
            author.copy(
                update={
                    "age": author.age + 1,
                    "updated_by": user_uuid,
                    "updated_at": ANY,
                }
            )
            for author in get_expected_authors(persisted_authors)
        ]

        actual_authors = await finalize(
            run_test=lambda Mutation: Mutation(Author).many_function(
                "inc_all_authors_age",
                args={
                    "author_uuids": [author.uuid for author in persisted_authors],
                    "user_uuid": user_uuid,
                },
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


@fixture
def persisted_authors(user_uuid: UUID, session: Client, session_async: AsyncClient):
    delete_all(session=session)
    return persist_authors(
        user_uuid,
        num_authors=20,
        # Note that update and delete tests use `authors.pop()` to prevent
        # interdependent test cases
        session=session,
        session_async=session_async,
    )
