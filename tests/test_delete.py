from typing import Any, Callable, cast
from uuid import UUID, uuid4

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo import Delete, Query
from cuckoo.errors import RecordNotFoundError
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
from tests.fixture.query_fixture import AUTHOR_ARTICLE_COMMENT_CONDITIONALS
from tests.fixture.sample_models import Author


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Delete).returning_one())
class TestOneByPK:
    async def test_deleting_a_model_if_record_exists(
        self,
        finalize: FinalizeReturning[Delete, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        author_to_delete = persisted_authors.pop()
        existing_uuid = cast(UUID, author_to_delete.uuid)

        author = await finalize(
            run_test=lambda Delete: Delete(Author).one_by_pk(uuid=existing_uuid),
            session=session,
            session_async=session_async,
        )

        assert isinstance(author, Author)
        assert author.uuid == existing_uuid
        assert (
            Query(Author, session=session, session_async=session_async)
            .aggregate(where={"uuid": {"_eq": existing_uuid}})
            .count()
            == 0
        )

    async def test_not_found_error_is_raised_if_record_does_not_exist(
        self,
        finalize: FinalizeReturning[Delete, Author],
        session: Client,
        session_async: AsyncClient,
    ):
        non_existend_uuid = uuid4()

        with raises(RecordNotFoundError):
            await finalize(
                run_test=lambda Delete: Delete(Author).one_by_pk(
                    uuid=non_existend_uuid
                ),
                session=session,
                session_async=session_async,
            )

    @mark.skip(reason="hasura bug?")
    @mark.parametrize(**ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Delete, Author],
        persisted_authors: list[Author],
        get_article_conditional: Callable[[Author], dict[str, Any]],
        get_comment_conditional: Callable[[Author], dict[str, Any]],
        get_expected_author: Callable[[Author], Author],
        session: Client,
        session_async: AsyncClient,
    ):
        author_to_delete = persisted_authors.pop()
        existing_uuid = cast(UUID, author_to_delete.uuid)
        assert existing_uuid
        expected_author = get_expected_author(author_to_delete)

        actual_author = await finalize(
            run_test=lambda Delete: Delete(Author).one_by_pk(uuid=existing_uuid),
            columns=all_columns(
                article_args=get_article_conditional(author_to_delete),
                comment_args=get_comment_conditional(author_to_delete),
            ),
            session=session,
            session_async=session_async,
        )

        assert_authors_ordered([actual_author], [expected_author])

    async def test_returning_default_column(
        self,
        finalize: FinalizeReturning[Delete, Author],
        persisted_authors: list[Author],
        session: Client,
        session_async: AsyncClient,
    ):
        author_to_delete = persisted_authors.pop()
        existing_uuid = cast(UUID, author_to_delete.uuid)
        assert existing_uuid

        actual_author = await finalize(
            run_test=lambda Delete: Delete(Author).one_by_pk(uuid=existing_uuid),
            session=session,
            session_async=session_async,
        )

        assert actual_author.uuid == existing_uuid


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Delete).returning_many())
class TestMany:
    async def test_returning_an_empty_list_if_non_matching_condition_is_provided(
        self,
        finalize: FinalizeReturning[Delete, list[Author]],
        session: Client,
        session_async: AsyncClient,
    ):
        actual_authors = await finalize(
            run_test=lambda Delete: Delete(Author).many(
                where={"name": {"_eq": "non existing"}}
            ),
            session=session,
            session_async=session_async,
        )

        assert isinstance(actual_authors, list)
        assert len(actual_authors) == 0

    @mark.skip(reason="hasura bug?")
    @mark.parametrize(**AUTHOR_ARTICLE_COMMENT_CONDITIONALS)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Delete, list[Author]],
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
            run_test=lambda Delete: Delete(Author).many(
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


@fixture(scope="module")
def persisted_authors(user_uuid: UUID, session: Client, session_async: AsyncClient):
    delete_all(session=session)
    return persist_authors(user_uuid, session=session, session_async=session_async)
