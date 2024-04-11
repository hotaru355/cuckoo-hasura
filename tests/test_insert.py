from typing import Callable
from uuid import UUID

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo import Delete, Insert, Query
from cuckoo.errors import InsertFailedError
from tests.fixture.common_fixture import (
    FinalizeAffectedRows,
    FinalizeParams,
    FinalizeReturning,
)
from tests.fixture.insert_fixture import (
    COLUMNS_ARG,
    INPUT_DATA,
    INPUT_DATA_LIST,
)
from tests.fixture.sample_models.public import Author


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Insert).returning_one())
class TestInsertOne:
    @mark.parametrize(**COLUMNS_ARG)
    @mark.parametrize(**INPUT_DATA)
    async def test_inserting_data(
        self,
        finalize: FinalizeReturning[Insert, Author],
        get_input_data: Callable[[UUID], dict],
        get_columns_arg: Callable[[], dict],
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        author = await finalize(
            run_test=lambda Insert: Insert(Author).one(data=get_input_data(user_uuid)),
            **get_columns_arg(),
            session=session,
            session_async=session_async,
        )

        assert author.uuid
        assert (
            Query(Author, session=session, session_async=session_async)
            .one_by_pk(author.uuid)
            .returning(**get_columns_arg())
            .dict(exclude_unset=True)
        ) == author.dict(exclude_unset=True)

    async def test_upserting_data(
        self,
        finalize: FinalizeReturning[Insert, Author],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        author = await finalize(
            lambda Insert: Insert(Author).one(
                data={
                    "uuid": persisted_author.uuid,
                    "name": "updated name",
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                },
                on_conflict={
                    "constraint": "authors_pkey",
                    "update_columns": ["name"],
                },
            ),
            columns=["uuid", "name"],
            session=session,
            session_async=session_async,
        )

        assert author.uuid
        assert author.uuid == persisted_author.uuid
        assert author.name == "updated name"
        assert (
            Query(Author, session=session, session_async=session_async)
            .one_by_pk(author.uuid)
            .returning(["name"])
            .name
            == "updated name"
        )

    async def test_upserting_data_with_matching_condition(
        self,
        finalize: FinalizeReturning[Insert, Author],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        author_updated = await finalize(
            lambda Insert: Insert(Author).one(
                data={
                    "uuid": persisted_author.uuid,
                    "name": "updated name",
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                },
                on_conflict={
                    "constraint": "authors_pkey",
                    "update_columns": ["name"],
                    "where": {
                        "age": {"_eq": persisted_author.age},
                    },
                },
            ),
            columns=["uuid", "name"],
            session=session,
            session_async=session_async,
        )

        assert author_updated.uuid
        assert author_updated.uuid == persisted_author.uuid
        assert author_updated.name == "updated name"
        assert (
            Query(Author, session=session, session_async=session_async)
            .one_by_pk(author_updated.uuid)
            .returning(["name"])
            .name
            == "updated name"
        )

    async def test_upserting_data_with_non_matching_condition(
        self,
        finalize: FinalizeReturning[Insert, Author],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        with raises(InsertFailedError):
            await finalize(
                lambda Insert: Insert(Author).one(
                    data={
                        "uuid": persisted_author.uuid,
                        "name": "NOT updated name",
                        "created_by": user_uuid,
                        "updated_by": user_uuid,
                    },
                    on_conflict={
                        "constraint": "authors_pkey",
                        "update_columns": ["name"],
                        "where": {
                            "age": {"_eq": -1},
                        },
                    },
                ),
                columns=["uuid", "name"],
                session=session,
                session_async=session_async,
            )


@mark.asyncio(scope="session")
class TestInsertMany:
    @mark.parametrize(**FinalizeParams(Insert).returning_many())
    @mark.parametrize(**COLUMNS_ARG)
    @mark.parametrize(**INPUT_DATA_LIST)
    async def test_returning_all_fields_and_relations_with_conditions(
        self,
        finalize: FinalizeReturning[Insert, list[Author]],
        get_data_list: Callable[[UUID], list[dict]],
        get_columns_arg: Callable[[], dict],
        user_uuid: UUID,
        expected_affected_rows: None,
        session: Client,
        session_async: AsyncClient,
    ):
        data = get_data_list(user_uuid)

        authors = await finalize(
            run_test=lambda Insert: Insert(Author).many(data=data),
            **get_columns_arg(),
            session=session,
            session_async=session_async,
        )

        expected_authors = (
            Query(Author, session=session, session_async=session_async)
            .many(where={"uuid": {"_in": [author.uuid for author in authors]}})
            .returning(**get_columns_arg())
        )
        assert len(authors) == len(data)
        for author in authors:
            expected_author = next(
                filter(lambda auth: auth.uuid == author.uuid, expected_authors)
            )
            assert expected_author.dict(exclude_unset=True) == author.dict(
                exclude_unset=True
            )

    @mark.parametrize(**FinalizeParams(Insert).affected_rows())
    @mark.parametrize(**INPUT_DATA_LIST)
    async def test_affected_rows(
        self,
        finalize: FinalizeAffectedRows[Insert, int],
        get_data_list: Callable[[UUID], list[dict]],
        expected_affected_rows: int,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        data = get_data_list(user_uuid)

        actual_affected_rows = await finalize(
            run_test=lambda Insert: Insert(Author).many(data=data),
            session=session,
            session_async=session_async,
        )

        assert actual_affected_rows == expected_affected_rows

    @mark.parametrize(**FinalizeParams(Insert).returning_many())
    async def test_upserting_data(
        self,
        finalize: FinalizeReturning[Insert, list[Author]],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        authors = await finalize(
            lambda Insert: Insert(Author).many(
                data=[
                    {
                        "uuid": persisted_author.uuid,
                        "name": "updated name",
                        "created_by": user_uuid,
                        "updated_by": user_uuid,
                    }
                ],
                on_conflict={
                    "constraint": "authors_pkey",
                    "update_columns": ["name"],
                },
            ),
            columns=["uuid", "name"],
            session=session,
            session_async=session_async,
        )

        assert authors[0].uuid
        assert authors[0].uuid == persisted_author.uuid
        assert authors[0].name == "updated name"
        assert (
            Query(Author, session=session, session_async=session_async)
            .one_by_pk(authors[0].uuid)
            .returning(["name"])
            .name
            == "updated name"
        )

    @mark.parametrize(**FinalizeParams(Insert).returning_many())
    async def test_upserting_data_with_matching_condition(
        self,
        finalize: FinalizeReturning[Insert, list[Author]],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        authors = await finalize(
            lambda Insert: Insert(Author).many(
                data=[
                    {
                        "uuid": persisted_author.uuid,
                        "name": "updated name",
                        "created_by": user_uuid,
                        "updated_by": user_uuid,
                    }
                ],
                on_conflict={
                    "constraint": "authors_pkey",
                    "update_columns": ["name"],
                    "where": {
                        "age": {"_eq": persisted_author.age},
                    },
                },
            ),
            columns=["uuid", "name"],
            session=session,
            session_async=session_async,
        )

        assert authors[0].uuid
        assert authors[0].uuid == persisted_author.uuid
        assert authors[0].name == "updated name"
        assert (
            Query(Author, session=session, session_async=session_async)
            .one_by_pk(authors[0].uuid)
            .returning(["name"])
            .name
            == "updated name"
        )

    @mark.parametrize(**FinalizeParams(Insert).returning_many())
    async def test_upserting_data_with_non_matching_condition(
        self,
        finalize: FinalizeReturning[Insert, Author],
        persisted_author: Author,
        user_uuid: UUID,
        session: Client,
        session_async: AsyncClient,
    ):
        assert persisted_author.name == "author_1"

        authors = await finalize(
            lambda Insert: Insert(Author).many(
                data=[
                    {
                        "uuid": persisted_author.uuid,
                        "name": "NOT updated name",
                        "created_by": user_uuid,
                        "updated_by": user_uuid,
                    }
                ],
                on_conflict={
                    "constraint": "authors_pkey",
                    "update_columns": ["name"],
                    "where": {
                        "age": {"_eq": -1},
                    },
                },
            ),
            columns=["uuid", "name"],
            session=session,
            session_async=session_async,
        )

        assert authors == []


@fixture(scope="function")
def persisted_author(
    user_uuid,
    session: Client,
):
    author = (
        Insert(Author, session=session)
        .one(
            {
                "name": "author_1",
                "age": 100,
                "created_by": user_uuid,
                "updated_by": user_uuid,
            }
        )
        .returning(["uuid", "name", "age"])
    )

    yield author

    Delete(Author, session=session).one_by_pk(uuid=author.uuid).returning()
