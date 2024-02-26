from typing import Generator, Type
from uuid import UUID

from httpx import Client

from cuckoo import Query
from cuckoo.delete import BatchDelete
from cuckoo.insert import BatchInsert
from cuckoo.update import BatchUpdate
from tests.fixture.common_fixture import (
    ParameterizeArgs,
)
from tests.fixture.common_utils import (
    all_columns,
    assert_authors_ordered,
    assert_authors_unordered,
)
from tests.fixture.sample_models.public import Article, Author, AuthorDetail, Comment


def insert_one(persisted_authors: list[Author], session: Client):
    def run_mutation(BatchInsert: Type[BatchInsert], _, __, user_uuid):
        return (
            BatchInsert(Author)
            .one(
                data=Author(
                    name="author_1",
                    age=100,
                    created_by=user_uuid,
                    updated_by=user_uuid,
                    detail=AuthorDetail(
                        country="Canada",
                        created_by=user_uuid,
                        updated_by=user_uuid,
                    ),
                    articles=[
                        Article(
                            title=f"some title {article_counter + 1}",
                            word_count=(article_counter % 3 + 1) * 1000,
                            created_by=user_uuid,
                            updated_by=user_uuid,
                            comments=[
                                Comment(
                                    content=f"some content {comment_counter + 1}",
                                    likes=(comment_counter % 3 + 1),
                                    created_by=user_uuid,
                                    updated_by=user_uuid,
                                )
                                for comment_counter in range(5)
                            ],
                        )
                        for article_counter in range(5)
                    ],
                ).to_hasura_input()
            )
            .yielding(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )

    def assert_model(actual_author_gen: Generator[Author, None, None]):
        actual_author = next(actual_author_gen)
        expected_author = (
            Query(Author, session=session)
            .one_by_pk(uuid=actual_author.uuid)
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )

        assert actual_author.uuid not in {
            persisted_author.uuid for persisted_author in persisted_authors
        }
        assert_authors_ordered([actual_author], [expected_author])

    return run_mutation, assert_model


def insert_many(persisted_authors: list[Author], session: Client):
    def run_mutation(BatchInsert: Type[BatchInsert], _, __, user_uuid: UUID):
        def get_insert():
            return BatchInsert(Author).many(
                data=[
                    Author(
                        name=f"author_{author_counter +1}",
                        age=100,
                        created_by=user_uuid,
                        updated_by=user_uuid,
                        detail=AuthorDetail(
                            country="Canada",
                            created_by=user_uuid,
                            updated_by=user_uuid,
                        ),
                        articles=[
                            Article(
                                title=f"some title {article_counter + 1}",
                                word_count=(article_counter % 3 + 1) * 1000,
                                created_by=user_uuid,
                                updated_by=user_uuid,
                                comments=[
                                    Comment(
                                        content=f"some content {comment_counter + 1}",
                                        likes=(comment_counter % 3 + 1),
                                        created_by=user_uuid,
                                        updated_by=user_uuid,
                                    )
                                    for comment_counter in range(5)
                                ],
                            )
                            for article_counter in range(5)
                        ],
                    ).to_hasura_input()
                    for author_counter in range(5)
                ]
            )

        return (
            get_insert().yielding(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
            get_insert().yield_affected_rows(),
            get_insert().yielding_with_rows(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
        )

    def assert_model(
        actual_author_results: tuple[
            Generator[Author, None, None],
            Generator[int, None, None],
            tuple[Generator[Author, None, None], Generator[int, None, None]],
        ],
    ):
        # .yielding
        actual_authors = list(actual_author_results[0])
        expected_authors = (
            Query(Author, session=session)
            .many(
                where={
                    "uuid": {
                        "_in": [actual_author.uuid for actual_author in actual_authors]
                    }
                }
            )
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        assert not {
            actual_author.uuid for actual_author in actual_authors
        }.intersection(
            {persisted_author.uuid for persisted_author in persisted_authors}
        )
        assert_authors_unordered(actual_authors, expected_authors)

        # .yield_affected_rows
        actual_num = next(actual_author_results[1])
        assert actual_num == 160, "5 authors + 5 details + 25 articles + 125 comments"

        # .yielding_with_rows
        actual_authors, actual_num = (
            list(actual_author_results[2][0]),
            next(actual_author_results[2][1]),
        )
        expected_authors = (
            Query(Author, session=session)
            .many(
                where={
                    "uuid": {
                        "_in": [actual_author.uuid for actual_author in actual_authors]
                    }
                }
            )
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        assert not {
            actual_author.uuid for actual_author in actual_authors
        }.intersection(
            {persisted_author.uuid for persisted_author in persisted_authors}
        )
        assert_authors_unordered(actual_authors, expected_authors)
        assert actual_num == 160, (
            "Expected: 5 authors + 5 details + 25 articles + 125 comments = 160\n"
            f"Found: {actual_num}"
        )

    return run_mutation, assert_model


def update_one(persisted_authors: list[Author], session: Client):
    author_to_update = persisted_authors.pop()

    def run_mutation(_, BatchUpdate: Type[BatchUpdate], __, ___):
        return (
            BatchUpdate(Author)
            .one_by_pk(
                pk_columns={
                    "uuid": author_to_update.uuid,
                },
                data={"name": "updated"},
            )
            .yielding(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )

    def assert_model(actual_author_gen: Generator[Author, None, None]):
        actual_author = next(actual_author_gen)
        expected_author = (
            Query(Author, session=session)
            .one_by_pk(uuid=author_to_update.uuid)
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )

        assert actual_author.name == "updated"
        assert_authors_ordered([actual_author], [expected_author])

    return run_mutation, assert_model


def update_many(persisted_authors: list[Author], session: Client):
    authors_yielding = [persisted_authors.pop(), persisted_authors.pop()]
    authors_affected_rows = [persisted_authors.pop(), persisted_authors.pop()]
    authors_with_rows = [persisted_authors.pop(), persisted_authors.pop()]

    def run_mutation(_, BatchUpdate: Type[BatchUpdate], __, ___):
        def get_update(authors: list[Author]):
            return BatchUpdate(Author).many(
                where={
                    "uuid": {"_in": [author.uuid for author in authors]},
                },
                data={"name": "updated"},
            )

        return (
            get_update(authors_yielding).yielding(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
            get_update(authors_affected_rows).yield_affected_rows(),
            get_update(authors_with_rows).yielding_with_rows(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
        )

    def assert_model(
        actual_author_results: tuple[
            Generator[Author, None, None],
            Generator[int, None, None],
            tuple[Generator[Author, None, None], Generator[int, None, None]],
        ],
    ):
        # .yielding
        actual_authors = list(actual_author_results[0])
        expected_authors = (
            Query(Author, session=session)
            .many(where={"uuid": {"_in": [author.uuid for author in authors_yielding]}})
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        for actual_author in actual_authors:
            assert actual_author.name == "updated"
        assert_authors_unordered(actual_authors, expected_authors)

        # .yield_affected_rows
        actual_num = next(actual_author_results[1])
        assert actual_num == len(
            authors_affected_rows
        ), f"Expected: {len(authors_affected_rows)} authors\nFound: {actual_num}"

        # .yielding_with_rows
        actual_authors, actual_num = (
            list(actual_author_results[2][0]),
            next(actual_author_results[2][1]),
        )
        expected_authors = (
            Query(Author, session=session)
            .many(
                where={"uuid": {"_in": [author.uuid for author in authors_with_rows]}}
            )
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        for actual_author in actual_authors:
            assert actual_author.name == "updated"
        assert_authors_unordered(actual_authors, expected_authors)
        assert actual_num == len(
            authors_with_rows
        ), f"Expected: {authors_with_rows} authors\nFound: {actual_num}"

    return run_mutation, assert_model


def update_many_distinct(persisted_authors: list[Author], session: Client):
    authors_yielding = [
        [persisted_authors.pop(), persisted_authors.pop()],
        [persisted_authors.pop(), persisted_authors.pop()],
    ]
    authors_affected_rows = [
        [persisted_authors.pop()],
        [persisted_authors.pop(), persisted_authors.pop()],
    ]
    authors_with_rows = [
        [persisted_authors.pop()],
        [persisted_authors.pop(), persisted_authors.pop()],
    ]

    def run_mutation(_, BatchUpdate: Type[BatchUpdate], __, ___):
        def get_update(authors1: list[Author], authors2: list[Author]):
            return BatchUpdate(Author).many_distinct(
                updates=[
                    {
                        "where": {
                            "uuid": {"_in": [author.uuid for author in authors1]},
                        },
                        "_set": {"name": "updated1"},
                    },
                    {
                        "where": {
                            "uuid": {"_in": [author.uuid for author in authors2]},
                        },
                        "_set": {"name": "updated2"},
                    },
                ]
            )

        return (
            get_update(authors_yielding[0], authors_yielding[1]).yielding(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
            get_update(
                authors_affected_rows[0], authors_affected_rows[1]
            ).yield_affected_rows(),
            get_update(authors_with_rows[0], authors_with_rows[1]).yielding_with_rows(
                columns=all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            ),
        )

    def assert_model(
        actual_author_results: tuple[
            Generator[Generator[Author, None, None], None, None],
            Generator[int, None, None],
            Generator[
                tuple[Generator[Author, None, None], int],
                None,
                None,
            ],
        ],
    ):
        # .yielding
        expected_authors = (
            Query(Author, session=session)
            .many(
                where={
                    "uuid": {
                        "_in": [
                            author.uuid
                            for author in (authors_yielding[0] + authors_yielding[1])
                        ]
                    }
                }
            )
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        results1, results2 = list(actual_author_results[0])
        actual_authors1, actual_authors2 = list(results1), list(results2)
        for actual_author1 in actual_authors1:
            assert actual_author1.name == "updated1"
        for actual_author2 in actual_authors2:
            assert actual_author2.name == "updated2"
        assert_authors_unordered(actual_authors1 + actual_authors2, expected_authors)

        # .yield_affected_rows
        actual_num1, actual_num2 = list(actual_author_results[1])
        assert actual_num1 == len(
            authors_affected_rows[0]
        ), f"Expected: {len(authors_affected_rows[0])} authors\nFound: {actual_num1}"
        assert actual_num2 == len(
            authors_affected_rows[1]
        ), f"Expected: {len(authors_affected_rows[1])} authors\nFound: {actual_num2}"

        # .yielding_with_rows
        expected_authors = (
            Query(Author, session=session)
            .many(
                where={
                    "uuid": {
                        "_in": [
                            author.uuid
                            for author in (authors_with_rows[0] + authors_with_rows[1])
                        ]
                    }
                }
            )
            .returning(
                all_columns(
                    article_args={"order_by": {"uuid": "asc"}},
                    comment_args={"order_by": {"uuid": "asc"}},
                )
            )
        )
        results = list(actual_author_results[2])
        actual_authors1, actual_num1 = list(results[0][0]), results[0][1]
        actual_authors2, actual_num2 = list(results[1][0]), results[1][1]
        for actual_author1 in actual_authors1:
            assert actual_author1.name == "updated1"
        for actual_author2 in actual_authors2:
            assert actual_author2.name == "updated2"
        assert_authors_unordered(actual_authors1 + actual_authors2, expected_authors)
        assert actual_num1 == len(
            authors_with_rows[0]
        ), f"Expected: {len(authors_with_rows[0])} authors\nFound: {actual_num1}"
        assert actual_num2 == len(
            authors_with_rows[1]
        ), f"Expected: {len(authors_with_rows[1])} authors\nFound: {actual_num2}"

    return run_mutation, assert_model


def delete_one(persisted_authors: list[Author], session: Client):
    author_to_delete = persisted_authors.pop()

    def run_mutation(_, __, BatchDelete: Type[BatchDelete], ___):
        return (
            BatchDelete(Author)
            .one_by_pk(
                uuid=author_to_delete.uuid,
            )
            .yielding(
                columns=[],
                invert_selection=True,
                # columns=[
                #     "uuid",
                #     "name",
                #     "age",
                #     "jsonb_list",
                #     "jsonb_dict",
                # ]
            )
        )

    def assert_model(actual_author_gen: Generator[Author, None, None]):
        actual_author = next(actual_author_gen)

        assert (
            Query(Author, session=session)
            .aggregate(
                where={
                    "uuid": {"_eq": author_to_delete.uuid},
                }
            )
            .count()
        ) == 0
        assert actual_author == author_to_delete
        # assert actual_author.dict(exclude_unset=True) == author_to_delete.dict(
        #     include={
        #         "uuid",
        #         "name",
        #         "age",
        #         "jsonb_list",
        #         "jsonb_dict",
        #     }
        # )

    return run_mutation, assert_model


def delete_many(persisted_authors: list[Author], session: Client):
    authors_yielding = [persisted_authors.pop(), persisted_authors.pop()]
    authors_affected_rows = [persisted_authors.pop()]
    authors_with_rows = [
        persisted_authors.pop(),
        persisted_authors.pop(),
        persisted_authors.pop(),
    ]

    def run_mutation(_, __, BatchDelete: Type[BatchDelete], ___):
        def get_delete(authors: list[Author]):
            return BatchDelete(Author).many(
                where={"uuid": {"_in": [author.uuid for author in authors]}}
            )

        return (
            get_delete(authors_yielding).yielding(
                columns=[],
                invert_selection=True,
            ),
            get_delete(authors_affected_rows).yield_affected_rows(),
            get_delete(authors_with_rows).yielding_with_rows(
                columns=[], invert_selection=True
            ),
        )

    def assert_model(
        actual_author_results: tuple[
            Generator[Author, None, None],
            Generator[int, None, None],
            tuple[Generator[Author, None, None], Generator[int, None, None]],
        ],
    ):
        # .yielding
        actual_authors = list(actual_author_results[0])
        assert (
            Query(Author, session=session)
            .aggregate(
                where={"uuid": {"_in": [author.uuid for author in authors_yielding]}}
            )
            .count()
        ) == 0
        assert_authors_unordered(
            actual_authors,
            [
                author.copy(
                    exclude={
                        "detail",
                        "articles",
                        "articles_aggregate",
                    }
                )
                for author in authors_yielding
            ],
        )

        # .yield_affected_rows
        actual_num = next(actual_author_results[1])
        assert actual_num == len(
            authors_affected_rows
        ), f"Expected: {len(authors_affected_rows)} authors\nFound: {actual_num}"

        # .yielding_with_rows
        actual_authors, actual_num = (
            list(actual_author_results[2][0]),
            next(actual_author_results[2][1]),
        )
        assert (
            Query(Author, session=session)
            .aggregate(
                where={"uuid": {"_in": [author.uuid for author in authors_with_rows]}}
            )
            .count()
        ) == 0
        assert_authors_unordered(
            actual_authors,
            [
                author.copy(
                    exclude={
                        "detail",
                        "articles",
                        "articles_aggregate",
                    }
                )
                for author in authors_with_rows
            ],
        )
        assert actual_num == len(
            authors_with_rows
        ), f"Expected: {len(authors_with_rows)} authors\nFound: {actual_num}"

    return run_mutation, assert_model


MUTATIONS1: ParameterizeArgs = {
    "argnames": ["run_and_assert1"],
    "ids": [
        "BatchInsert.one()",
        "BatchInsert.many()",
        "BatchUpdate.one_by_pk()",
        "BatchUpdate.many()",
        "BatchUpdate.many_distinct()",
        "BatchDelete.one_by_pk()",
        "BatchDelete.many()",
    ],
    "argvalues": [
        [insert_one],
        [insert_many],
        [update_one],
        [update_many],
        [update_many_distinct],
        [delete_one],
        [delete_many],
    ],
}

MUTATIONS2: ParameterizeArgs = {
    **MUTATIONS1,
    "argnames": ["run_and_assert2"],
}
