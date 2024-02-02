from typing import Optional
from uuid import UUID

from httpx import AsyncClient, Client

from cuckoo import Delete, Include, Insert
from cuckoo.finalizers import AggregatesDict
from tests.fixture.sample_models.public import (
    Address,
    Article,
    Author,
    AuthorDetail,
    Comment,
    DetailsAddresses,
)

DEFAULT_COUNTS = {
    Author: 10,
    Article: 5,
    Comment: 5,
}


def delete_all(
    session: Optional[Client] = None,
):
    if session is None:
        session = Client(timeout=None)

    return Delete(Author, session=session).many(where={}).affected_rows()


def generate_author_data(
    user_uuid: UUID,
    num_authors=DEFAULT_COUNTS[Author],
    num_articles=DEFAULT_COUNTS[Article],
    num_comments=DEFAULT_COUNTS[Comment],
):
    return [
        Author(
            name=f"author_{author_counter}",
            age=(author_counter % 4 + 3) * 10,
            # [30, 40, 50, 60, 30, 40, 50, 60, 30, 40],
            jsonb_list=["a", "b", "c", "d"],
            jsonb_dict={"a": 1, "b": 2, "c": 3, "d": 4},
            created_by=user_uuid,
            updated_by=user_uuid,
            detail=AuthorDetail(
                country=["Canada", "USA", "Andorra"][author_counter % 3],
                created_by=user_uuid,
                updated_by=user_uuid,
            ),
            articles=(
                [
                    Article(
                        title=f"some title {article_counter + 1}",
                        word_count=(article_counter % 3 + 1) * 1000,
                        # [1000, 2000, 3000, 1000, 2000]
                        created_by=user_uuid,
                        updated_by=user_uuid,
                        comments=[
                            Comment(
                                content=f"some content {comment_counter + 1}",
                                likes=(comment_counter % 3 + 1),
                                # [1, 2, 3, 1, 2]
                                created_by=user_uuid,
                                updated_by=user_uuid,
                            )
                            for comment_counter in range(num_comments)
                        ],
                    )
                    for article_counter in range(num_articles)
                ]
                if num_articles > 0
                else None
            ),
        ).to_hasura_input()
        for author_counter in range(num_authors)
    ]


def persist_authors(
    user_uuid: UUID,
    num_authors=DEFAULT_COUNTS[Author],
    num_articles=DEFAULT_COUNTS[Article],
    num_comments=DEFAULT_COUNTS[Comment],
    session: Optional[Client] = None,
    session_async: Optional[AsyncClient] = None,
):
    if session is None:
        session = Client(timeout=None)
    if session_async is None:
        session_async = AsyncClient(timeout=None)

    authors = (
        Insert(Author, session=session, session_async=session_async)
        .many(
            data=generate_author_data(
                user_uuid=user_uuid,
                num_authors=num_authors,
                num_articles=num_articles,
                num_comments=num_comments,
            )
        )
        .returning(columns=all_columns())
    )
    assert len(authors) == num_authors
    for author in authors:
        assert author.uuid
        assert author.name
        assert author.age

        assert author.detail
        assert author.detail.uuid
        assert author.detail.country

        assert len(author.articles) == num_articles
        for article in author.articles:
            assert article.uuid
            assert article.title
            assert article.word_count

            assert len(article.comments) == num_comments
            for comment in article.comments:
                assert comment.uuid
                assert comment.content
                assert isinstance(comment.likes, int)

    return authors


def persist_author_details(
    user_uuid: UUID,
    num_author_details=1,
    num_past_primary_addresses=10,
    num_past_secondary_addresses=10,
    session: Optional[Client] = None,
    session_async: Optional[AsyncClient] = None,
):
    if session is None:
        session = Client(timeout=None)
    if session_async is None:
        session_async = AsyncClient(timeout=None)

    author = (
        Insert(Author, session=session, session_async=session_async)
        .one(
            data={
                "name": "test",
                "created_by": user_uuid,
                "updated_by": user_uuid,
            }
        )
        .returning()
    )
    author_details = (
        Insert(AuthorDetail, session=session, session_async=session_async)
        .many(
            data=[
                AuthorDetail(
                    author_uuid=author.uuid,
                    country=["Canada", "USA", "Andorra"][detail_counter % 3],
                    primary_address=Address(
                        street="primary street",
                        postal_code="primary code",
                        walk_score=round(
                            detail_counter / num_past_primary_addresses * 100, 1
                        ),
                        created_by=user_uuid,
                        updated_by=user_uuid,
                    ),
                    secondary_address=Address(
                        street="secondary street",
                        postal_code="secondary code",
                        walk_score=round(
                            detail_counter / num_past_primary_addresses * 100, 1
                        ),
                        created_by=user_uuid,
                        updated_by=user_uuid,
                    ),
                    past_primary_addresses=[
                        DetailsAddresses(
                            address=Address(
                                street=f"past primary street {prim_counter}",
                                postal_code=f"past primary code {prim_counter}",
                                walk_score=round(
                                    prim_counter / num_past_primary_addresses * 100, 1
                                ),
                                created_by=user_uuid,
                                updated_by=user_uuid,
                            ),
                            created_by=user_uuid,
                        )
                        for prim_counter in range(num_past_primary_addresses)
                    ],
                    past_secondary_addresses=[
                        DetailsAddresses(
                            is_primary=False,
                            address=Address(
                                street=f"past secondary street {sec_counter}",
                                postal_code=f"past secondary code {sec_counter}",
                                walk_score=round(
                                    sec_counter / num_past_primary_addresses * 100, 1
                                ),
                                created_by=user_uuid,
                                updated_by=user_uuid,
                            ),
                            created_by=user_uuid,
                        )
                        for sec_counter in range(num_past_secondary_addresses)
                    ],
                    created_by=user_uuid,
                    updated_by=user_uuid,
                ).to_hasura_input()
                for detail_counter in range(num_author_details)
            ],
        )
        .returning(
            columns=[
                "uuid",
                "country",
                (
                    Include(Address, field_name="primary_address")
                    .one()
                    .returning(
                        columns=[
                            "uuid",
                            "street",
                            "postal_code",
                            "walk_score",
                        ]
                    )
                ),
                (
                    Include(Address, field_name="secondary_address")
                    .one()
                    .returning(
                        columns=[
                            "uuid",
                            "street",
                            "postal_code",
                            "walk_score",
                        ]
                    )
                ),
                (
                    Include(DetailsAddresses, field_name="past_primary_addresses")
                    .many(where={"is_primary": {"_eq": True}})
                    .returning(
                        columns=[
                            "uuid",
                            Include(Address)
                            .one()
                            .returning(
                                columns=[
                                    "uuid",
                                    "street",
                                    "postal_code",
                                    "walk_score",
                                ]
                            ),
                        ]
                    )
                ),
                (
                    Include(DetailsAddresses, field_name="past_secondary_addresses")
                    .many(where={"is_primary": {"_eq": False}})
                    .returning(
                        columns=[
                            "uuid",
                            Include(Address)
                            .one()
                            .returning(
                                columns=[
                                    "uuid",
                                    "street",
                                    "postal_code",
                                    "walk_score",
                                ]
                            ),
                        ]
                    )
                ),
            ]
        )
    )
    assert len(author_details) == num_author_details
    for detail in author_details:
        assert detail.uuid
        assert detail.country

        assert detail.primary_address.uuid
        assert detail.primary_address.street
        assert detail.primary_address.postal_code
        assert detail.primary_address.walk_score is not None

        assert detail.secondary_address.uuid
        assert detail.secondary_address.street
        assert detail.secondary_address.postal_code
        assert detail.secondary_address.walk_score is not None

        assert len(detail.past_primary_addresses) == num_past_primary_addresses
        for past_address in detail.past_primary_addresses:
            assert past_address.uuid
            assert past_address.address.uuid
            assert past_address.address.street
            assert past_address.address.postal_code
            assert past_address.address.walk_score is not None

        assert len(detail.past_secondary_addresses) == num_past_secondary_addresses
        for past_address in detail.past_secondary_addresses:
            assert past_address.uuid
            assert past_address.address.uuid
            assert past_address.address.street
            assert past_address.address.postal_code
            assert past_address.address.walk_score is not None

    return author_details


def assert_authors_unordered(
    actual_authors: list[Author],
    expected_authors: list[Author],
):
    assert len(actual_authors) == len(expected_authors)

    for actual_author in actual_authors:
        expected_author = next(
            filter(lambda author: author.uuid == actual_author.uuid, expected_authors),
            None,
        )
        assert expected_author
        assert_authors_ordered([actual_author], [expected_author])


def assert_authors_ordered(
    actual_authors: list[Author],
    expected_authors: list[Author],
):
    assert len(actual_authors) == len(expected_authors)

    for actual_author, expected_author in zip(actual_authors, expected_authors):
        assert actual_author.dict(exclude_unset=True) == expected_author.dict(
            exclude_unset=True
        ), (
            f"Expected: {expected_author.dict(exclude_unset=True)}\n"
            f"Found: {actual_author.dict(exclude_unset=True)}"
        )


def all_columns(
    article_args={},
    comment_args={},
    args_aggr_article=None,
    args_aggr_on_article: AggregatesDict = {"count": True},
    arg_aggr_comments=None,
    arg_aggr_on_comments: AggregatesDict = {"count": True},
):
    if not args_aggr_article:
        args_aggr_article = article_args
    if not arg_aggr_comments:
        arg_aggr_comments = comment_args
    return [
        "uuid",
        "name",
        "age",
        "home_zone",
        "jsonb_list",
        "jsonb_dict",
        "updated_by",
        "updated_at",
        "created_by",
        "created_at",
        "deleted_by",
        "deleted_at",
        Include(AuthorDetail).one().returning(["uuid", "country"]),
        (
            Include(Article)
            .many(**article_args)
            .returning(
                [
                    "uuid",
                    "title",
                    "word_count",
                    (
                        Include(Comment)
                        .many(**comment_args)
                        .returning(["uuid", "content", "likes"])
                    ),
                    (
                        Include(Comment)
                        .aggregate(**arg_aggr_comments)
                        .on(**arg_aggr_on_comments)
                    ),
                ]
            )
        ),
        (Include(Article).aggregate(**args_aggr_article).on(**args_aggr_on_article)),
    ]
