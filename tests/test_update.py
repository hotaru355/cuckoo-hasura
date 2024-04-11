from typing import Any, Callable
from uuid import UUID, uuid4

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo import Update
from cuckoo.errors import RecordNotFoundError
from tests.fixture.common_fixture import (
    ARTICLE_COMMENT_CONDITIONALS,
    FinalizeAffectedRows,
    FinalizeParams,
    FinalizeReturning,
)
from tests.fixture.common_utils import (
    all_columns,
    assert_authors_ordered,
    assert_authors_unordered,
    delete_all,
    persist_authors,
)
from tests.fixture.sample_models import Author
from tests.fixture.update_fixture import (
    AUTHOR_ARTICLE_COMMENT_CONDITIONALS,
    UPDATE_ARGS,
    UPDATE_DISTINCT_ARGS,
)


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Update).returning_one())
class TestOneByPK:
    @mark.parametrize(**UPDATE_ARGS)
    async def test_updating_an_existing_model(
        self,
        finalize: FinalizeReturning[Update, Author],
        persisted_authors: list[Author],
        args: dict,
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_author = get_expected_author(persisted_authors[0])
        existing_uuid = persisted_authors[0].uuid

        actual_author = await finalize(
            run_test=lambda Update: Update(Author).one_by_pk(
                pk_columns={
                    "uuid": existing_uuid,
                },
                **args,
            ),
            columns=[
                "uuid",
                "age",
                "jsonb_list",
                "jsonb_dict",
            ],
            session=session,
            session_async=session_async,
        )

        assert actual_author.dict(exclude_unset=True) == expected_author.dict(
            exclude_unset=True
        )

    async def test_not_found_error_is_raised_if_record_to_update_does_not_exist(
        self,
        finalize: FinalizeReturning[Update, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        non_existing_uuid = uuid4()

        with raises(RecordNotFoundError):
            await finalize(
                run_test=lambda Update: Update(Author).one_by_pk(
                    pk_columns={
                        "uuid": non_existing_uuid,
                    },
                    data={"age": 34},
                ),
                session=session,
                session_async=session_async,
            )

    async def test_value_error_is_raised_if_no_data_to_update_is_provided(
        self,
        persisted_authors: list[Author],
        finalize: FinalizeReturning[Update, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        existing_uuid = persisted_authors[0].uuid

        with raises(ValueError):
            await finalize(
                run_test=lambda Update: Update(Author).one_by_pk(
                    pk_columns={
                        "uuid": existing_uuid,
                    },
                    # no data
                ),
                session=session,
                session_async=session_async,
            )

    @mark.parametrize(**ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations(
        self,
        finalize: FinalizeReturning[Update, Author],
        persisted_authors: list[Author],
        get_article_conditional: Callable[[Author], dict[str, Any]],
        get_comment_conditional: Callable[[Author], dict[str, Any]],
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        some_author = persisted_authors[0]
        expected_author = get_expected_author(some_author)
        expected_author.name = "updated"  # do not put in fixture, as fixture is shared

        actual_author = await finalize(
            run_test=lambda Update: Update(Author).one_by_pk(
                pk_columns={"uuid": some_author.uuid},
                data={"name": "updated"},
            ),
            columns=all_columns(
                article_args=get_article_conditional(some_author),
                comment_args=get_comment_conditional(some_author),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered(
            [actual_author.copy(exclude={"updated_at"})],
            [expected_author.copy(exclude={"updated_at"})],
        )
        assert actual_author.updated_at is not None
        assert expected_author.updated_at is not None
        assert actual_author.updated_at > expected_author.updated_at

    async def test_returning_default_column(
        self,
        finalize: FinalizeReturning[Update, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_author = await finalize(
            run_test=lambda Update: Update(Author).one_by_pk(
                pk_columns={"uuid": persisted_authors[0].uuid},
                data={"name": "updated"},
            ),
            # no columns
            session=session,
            session_async=session_async,
        )

        assert actual_author.uuid == persisted_authors[0].uuid


@mark.asyncio(scope="session")
class TestMany:
    @mark.parametrize(**FinalizeParams(Update).returning_many())
    @mark.parametrize(**UPDATE_ARGS)
    async def test_updating_an_existing_model(
        self,
        finalize: FinalizeReturning[Update, list[Author]],
        persisted_authors: list[Author],
        args: dict,
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_author = get_expected_author(persisted_authors[0])
        existing_uuid = persisted_authors[0].uuid

        actual_authors = await finalize(
            run_test=lambda Update: Update(Author).many(
                where={
                    "uuid": {"_eq": existing_uuid},
                },
                **args,
            ),
            columns=[
                "uuid",
                "age",
                "jsonb_list",
                "jsonb_dict",
            ],
            session=session,
            session_async=session_async,
        )

        assert actual_authors[0].dict(exclude_unset=True) == expected_author.dict(
            exclude_unset=True
        )

    @mark.parametrize(**FinalizeParams(Update).returning_many())
    async def test_updating_all_records_with_empty_condition(
        self,
        finalize: FinalizeReturning[Update, list[Author]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Update: Update(Author).many(
                where={},
                data={"name": "updated"},
            ),
            columns=["name"],
            session=session,
            session_async=session_async,
        )

        assert len(actual_authors) == len(persisted_authors)
        for actual_author in actual_authors:
            assert actual_author.name == "updated"

    @mark.parametrize(**FinalizeParams(Update).returning_many())
    async def test_returning_an_empty_list_if_non_matching_condition_is_provided(
        self,
        finalize: FinalizeReturning[Update, list[Author]],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Update: Update(Author).many(
                where={"name": {"_eq": "non existing"}},
                data={"name": "will not update any"},
            ),
            session=session,
            session_async=session_async,
        )

        assert isinstance(actual_authors, list)
        assert len(actual_authors) == 0

    @mark.parametrize(**FinalizeParams(Update).returning_many())
    @mark.parametrize(**AUTHOR_ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Update, list[Author]],
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
            run_test=lambda Update: Update(Author).many(
                **get_author_condition(persisted_authors),
                data={"updated_at": "now()"},
            ),
            columns=all_columns(
                article_args=get_article_conditional(persisted_authors),
                comment_args=get_comment_conditional(persisted_authors),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors(actual_authors, expected_authors)

    @mark.parametrize(**FinalizeParams(Update).affected_rows())
    async def test_affected_rows(
        self,
        finalize: FinalizeAffectedRows[Update, int],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_affected_rows = 3

        actual_affected_rows = await finalize(
            run_test=lambda Update: Update(Author).many(
                where={"age": {"_eq": 30}},
                data={"name": "updated"},
            ),
            session=session,
            session_async=session_async,
        )

        assert actual_affected_rows == expected_affected_rows


@mark.asyncio(scope="session")
class TestManyDistinct:
    @mark.parametrize(**FinalizeParams(Update).returning_many_distinct())
    @mark.parametrize(**UPDATE_DISTINCT_ARGS)
    async def test_updating_an_existing_model(
        self,
        finalize: FinalizeReturning[Update, list[list[Author]]],
        persisted_authors: list[Author],
        args: dict,
        get_expected_authors: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_authors_30 = [
            get_expected_authors(persisted_author)
            for persisted_author in persisted_authors
            if persisted_author.age == 30
        ]
        assert expected_authors_30, "invalid fixture"
        expected_authors_50 = [
            get_expected_authors(persisted_author)
            for persisted_author in persisted_authors
            if persisted_author.age == 50
        ]
        assert expected_authors_50, "invalid fixture"

        actual_authors_30, actual_authors_50 = await finalize(
            run_test=lambda Update: Update(Author).many_distinct(
                updates=[
                    {
                        "where": {
                            "age": {"_eq": 30},
                        },
                        **args,
                    },
                    {
                        "where": {
                            "age": {"_eq": 50},
                        },
                        **args,
                    },
                ]
            ),
            columns=[
                "uuid",
                "age",
                "jsonb_list",
                "jsonb_dict",
            ],
            session=session,
            session_async=session_async,
        )

        assert_authors_unordered(actual_authors_30, expected_authors_30)
        assert_authors_unordered(actual_authors_50, expected_authors_50)

    @mark.parametrize(**FinalizeParams(Update).affected_rows_distinct())
    async def test_affected_rows(
        self,
        finalize: FinalizeAffectedRows[Update, list[int]],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        expected_affected_rows_30 = len(
            [
                persisted_author
                for persisted_author in persisted_authors
                if persisted_author.age == 30
            ]
        )
        assert expected_affected_rows_30, "invalid fixture"
        expected_affected_rows_50 = len(
            [
                persisted_author
                for persisted_author in persisted_authors
                if persisted_author.age == 50
            ]
        )
        assert expected_affected_rows_50, "invalid fixture"

        actual_affected_rows_30, actual_affected_rows_50 = await finalize(
            run_test=lambda Update: Update(Author).many_distinct(
                updates=[
                    {
                        "where": {
                            "age": {"_eq": 30},
                        },
                        "_set": {"age": 22},
                    },
                    {
                        "where": {
                            "age": {"_eq": 50},
                        },
                        "_set": {"age": 23},
                    },
                ]
            ),
            session=session,
            session_async=session_async,
        )

        assert actual_affected_rows_30 == expected_affected_rows_30
        assert actual_affected_rows_50 == expected_affected_rows_50


@fixture(scope="function")
def persisted_authors(user_uuid: UUID, session: Client, session_async: AsyncClient):
    delete_all(session=session)
    return persist_authors(user_uuid, session=session, session_async=session_async)
