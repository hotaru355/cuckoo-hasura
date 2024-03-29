from typing import List

from cuckoo.models import AggregateResponse
from tests.fixture.common_fixture import (
    ParameterizeArgs,
)
from tests.fixture.common_utils import (
    assert_authors_ordered,
)
from tests.fixture.sample_models.public import Author

UPDATE_ARGS: ParameterizeArgs = {
    "argnames": ["args", "get_expected_author"],
    "ids": [
        "data",
        "increment int",
        "prepend elem to list",
        "prepend list to list",
        # "prepend elem to dict",
        "prepend dict to dict",
        "append elem to list",
        "append list to list",
        # "append elem to dict",
        "append dict to dict",
        "delete elm from list",
        "delete key from dict",
        "delete elm from list by path",
        "delete key from dict by path",
    ],
    "argvalues": [
        [
            {"data": {"age": 22}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "jsonb_list",
                    "jsonb_dict",
                },
                update={"age": 22},
            ),
        ],
        [
            {"inc": {"age": 5}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "jsonb_list",
                    "jsonb_dict",
                },
                update={"age": author.age + 5},
            ),
        ],
        [
            {"prepend": {"jsonb_list": "z"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": ["z"] + author.jsonb_list,
                },
            ),
        ],
        [
            {"prepend": {"jsonb_list": ["x", "y"]}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": ["x", "y"] + author.jsonb_list,
                },
            ),
        ],
        [
            {"prepend": {"jsonb_dict": {"z": 10}}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {"z": 10, **author.jsonb_dict},
                },
            ),
        ],
        [
            {"append": {"jsonb_list": "z"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list + ["z"],
                },
            ),
        ],
        [
            {"append": {"jsonb_list": ["x", "y"]}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list + ["x", "y"],
                },
            ),
        ],
        [
            {"append": {"jsonb_dict": {"z": 10}}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {"z": 10, **author.jsonb_dict},
                },
            ),
        ],
        [
            {"delete_elem": {"jsonb_list": 2}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list[0:2] + author.jsonb_list[3:],
                },
            ),
        ],
        [
            {"delete_key": {"jsonb_dict": "c"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {
                        key: value
                        for key, value in author.jsonb_dict.items()
                        if key != "c"
                    },
                },
            ),
        ],
        [
            {"delete_at_path": {"jsonb_list": "2"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list[0:2] + author.jsonb_list[3:],
                },
            ),
        ],
        [
            {"delete_at_path": {"jsonb_dict": "c"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {
                        key: value
                        for key, value in author.jsonb_dict.items()
                        if key != "c"
                    },
                },
            ),
        ],
    ],
}

UPDATE_DISTINCT_ARGS: ParameterizeArgs = {
    "argnames": ["args", "get_expected_authors"],
    "ids": [
        "data",
        "increment int",
        "prepend elem to list",
        "prepend list to list",
        "prepend dict to dict",
        "append elem to list",
        "append list to list",
        "append dict to dict",
        "delete elm from list",
        "delete key from dict",
        "delete elm from list by path",
        "delete key from dict by path",
    ],
    "argvalues": [
        [
            {"_set": {"age": 22}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "jsonb_list",
                    "jsonb_dict",
                },
                update={"age": 22},
            ),
        ],
        [
            {"_inc": {"age": 5}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "jsonb_list",
                    "jsonb_dict",
                },
                update={"age": author.age + 5},
            ),
        ],
        [
            {"_prepend": {"jsonb_list": "z"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": ["z"] + author.jsonb_list,
                },
            ),
        ],
        [
            {"_prepend": {"jsonb_list": ["x", "y"]}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": ["x", "y"] + author.jsonb_list,
                },
            ),
        ],
        [
            {"_prepend": {"jsonb_dict": {"z": 10}}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {"z": 10, **author.jsonb_dict},
                },
            ),
        ],
        [
            {"_append": {"jsonb_list": "z"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list + ["z"],
                },
            ),
        ],
        [
            {"_append": {"jsonb_list": ["x", "y"]}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list + ["x", "y"],
                },
            ),
        ],
        [
            {"_append": {"jsonb_dict": {"z": 10}}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {"z": 10, **author.jsonb_dict},
                },
            ),
        ],
        [
            {"_delete_elem": {"jsonb_list": 2}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list[0:2] + author.jsonb_list[3:],
                },
            ),
        ],
        [
            {"_delete_key": {"jsonb_dict": "c"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {
                        key: value
                        for key, value in author.jsonb_dict.items()
                        if key != "c"
                    },
                },
            ),
        ],
        [
            {"_delete_at_path": {"jsonb_list": "2"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_dict",
                },
                update={
                    "jsonb_list": author.jsonb_list[0:2] + author.jsonb_list[3:],
                },
            ),
        ],
        [
            {"_delete_at_path": {"jsonb_dict": "c"}},
            lambda author: author.copy(
                include={
                    "uuid",
                    "age",
                    "jsonb_list",
                },
                update={
                    "jsonb_dict": {
                        key: value
                        for key, value in author.jsonb_dict.items()
                        if key != "c"
                    },
                },
            ),
        ],
    ],
}


def assert_authors_updated(
    actual_authors: List[Author], expected_authors: List[Author]
):
    assert len(actual_authors) == len(expected_authors)

    for actual_author in actual_authors:
        expected_author = next(
            filter(lambda author: author.uuid == actual_author.uuid, expected_authors),
            None,
        )
        assert expected_author
        assert_authors_ordered(
            [actual_author.copy(exclude={"updated_at"})],
            [expected_author.copy(exclude={"updated_at"})],
        )
        assert actual_author.updated_at > expected_author.updated_at


AUTHOR_ARTICLE_COMMENT_CONDITIONALS: ParameterizeArgs = {
    "argnames": [
        "get_author_condition",
        "get_article_conditional",
        "get_comment_conditional",
        "get_expected_authors",
        "assert_authors",
    ],
    "ids": ["empty where", "where", "order_by", "limit", "offset", "distinct_on"],
    "argvalues": [
        (
            lambda authors: {
                "where": {},
            },
            lambda authors: {},
            lambda authors: {},
            lambda authors: [
                author.copy(
                    update={
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
                )
                for author in authors
            ],
            assert_authors_updated,
        ),
        (
            lambda authors: {
                "where": {
                    "name": {"_eq": authors[2].name},
                }
            },
            lambda authors: {
                "where": {
                    "title": {"_eq": authors[2].articles[3].title},
                }
            },
            lambda authors: {
                "where": {
                    "content": {"_eq": authors[2].articles[3].comments[2].content},
                }
            },
            lambda authors: [
                authors[2].copy(
                    update={
                        "articles": [
                            authors[2]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[2].articles[3].comments[2].copy()
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 1}
                                    ),
                                }
                            )
                        ],
                        "articles_aggregate": AggregateResponse(aggregate={"count": 1}),
                    }
                )
            ],
            assert_authors_updated,
        ),
        (
            lambda authors: {
                "where": {
                    "uuid": {"_eq": authors[3].uuid},
                }
            },
            lambda authors: {
                "order_by": {"title": "desc"},
            },
            lambda authors: {
                "order_by": {"content": "desc"},
            },
            lambda authors: [
                authors[3].copy(
                    update={
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
                            for article in reversed(authors[3].articles)
                        ],
                        "articles_aggregate": AggregateResponse(aggregate={"count": 5}),
                    }
                )
            ],
            assert_authors_updated,
        ),
        (
            lambda authors: {
                "where": {
                    "uuid": {"_eq": authors[9].uuid},
                }
            },
            lambda authors: {
                "limit": 2,
                "order_by": {"title": "desc"},
            },
            lambda authors: {
                "limit": 3,
                "order_by": {"content": "desc"},
            },
            lambda authors: [
                authors[9].copy(
                    update={
                        "articles": [
                            authors[9]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[9].articles[4].comments[4].copy(),
                                        authors[9].articles[4].comments[3].copy(),
                                        authors[9].articles[4].comments[2].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[9]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[9].articles[3].comments[4].copy(),
                                        authors[9].articles[3].comments[3].copy(),
                                        authors[9].articles[3].comments[2].copy(),
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
            ],
            assert_authors_updated,
        ),
        (
            lambda authors: {
                "where": {
                    "uuid": {"_eq": authors[0].uuid},
                }
            },
            lambda authors: {
                "offset": 3,
                "order_by": {"title": "desc"},
            },
            lambda authors: {
                "offset": 2,
                "order_by": {"content": "desc"},
            },
            lambda authors: [
                authors[0].copy(
                    update={
                        "articles": [
                            authors[0]
                            .articles[1]
                            .copy(
                                update={
                                    "comments": [
                                        authors[0].articles[1].comments[2].copy(),
                                        authors[0].articles[1].comments[1].copy(),
                                        authors[0].articles[1].comments[0].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[0]
                            .articles[0]
                            .copy(
                                update={
                                    "comments": [
                                        authors[0].articles[0].comments[2].copy(),
                                        authors[0].articles[0].comments[1].copy(),
                                        authors[0].articles[0].comments[0].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                        ],
                        "articles_aggregate": AggregateResponse(aggregate={"count": 2}),
                    }
                )
            ],
            assert_authors_updated,
        ),
        (
            lambda authors: {
                "where": {
                    "uuid": {"_eq": authors[8].uuid},
                }
            },
            lambda authors: {
                "distinct_on": "word_count",
                "order_by": [
                    {"word_count": "asc"},
                    {"title": "desc"},
                ],
            },
            lambda authors: {
                "distinct_on": "likes",
                "order_by": [
                    {"likes": "desc"},
                    {"content": "desc"},
                ],
            },
            lambda authors: [
                authors[8].copy(
                    update={
                        "articles": [
                            authors[8]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[8].articles[3].comments[2].copy(),
                                        authors[8].articles[3].comments[4].copy(),
                                        authors[8].articles[3].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[8]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[8].articles[4].comments[2].copy(),
                                        authors[8].articles[4].comments[4].copy(),
                                        authors[8].articles[4].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[8]
                            .articles[2]
                            .copy(
                                update={
                                    "comments": [
                                        authors[8].articles[2].comments[2].copy(),
                                        authors[8].articles[2].comments[4].copy(),
                                        authors[8].articles[2].comments[3].copy(),
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
            ],
            assert_authors_updated,
        ),
    ],
}
