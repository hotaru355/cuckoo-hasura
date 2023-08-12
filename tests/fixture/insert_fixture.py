from geojson_pydantic import Polygon

from cuckoo import Include
from tests.fixture.common_fixture import ParameterizeArgs
from tests.fixture.sample_models.public import Author, Article, AuthorDetail, Comment


ARTICLE_COMMENT_CONDITIONALS: ParameterizeArgs = {
    "argnames": [
        "get_article_conditional",
        "get_comment_conditional",
    ],
    "ids": ["NONE", "where", "order_by", "limit", "offset", "distinct_on", "ALL"],
    "argvalues": [
        (
            lambda author: {},
            lambda author: {},
        ),
        (
            lambda author: {"where": {"title": {"_eq": author.articles[3].title}}},
            lambda author: {
                "where": {"content": {"_eq": author.articles[3].comments[2].content}}
            },
        ),
        (
            lambda author: {"order_by": {"title": "desc"}},
            lambda author: {"order_by": {"content": "desc"}},
        ),
        (
            lambda author: {"limit": 2, "order_by": {"title": "desc"}},
            lambda author: {"limit": 3, "order_by": {"content": "desc"}},
        ),
        (
            lambda author: {"offset": 3, "order_by": {"title": "desc"}},
            lambda author: {"offset": 2, "order_by": {"content": "desc"}},
        ),
        (
            lambda author: {
                "distinct_on": "word_count",
                "order_by": [{"word_count": "asc"}, {"title": "desc"}],
            },
            lambda author: {
                "distinct_on": "likes",
                "order_by": [{"likes": "desc"}, {"content": "desc"}],
            },
        ),
        (
            lambda author: {
                "where": {
                    "word_count": {"_lt": 3000},
                },
                "order_by": [{"word_count": "asc"}, {"title": "desc"}],
                "distinct_on": "word_count",
                "limit": 1,
                "offset": 1,
            },
            lambda author: {
                "where": {
                    "likes": {"_lt": 3},
                },
                "order_by": [{"likes": "desc"}, {"content": "desc"}],
                "distinct_on": "likes",
                "limit": 1,
                "offset": 1,
            },
        ),
    ],
}

AUTHOR_CONDITIONALS = [
    [
        lambda: {
            "columns": [
                "uuid",
                "name",
                "age",
                "home_zone",
                Include(AuthorDetail).one().returning(),
                (
                    Include(Article)
                    .many(**article_conditional)
                    .returning(
                        [
                            "uuid",
                            "title",
                            (Include(Comment).many().returning(["uuid", "content"])),
                            (Include(Comment).aggregate(**comment_conditional).count()),
                        ]
                    )
                ),
                (Include(Article).aggregate(**article_conditional).count()),
            ]
        }
    ]
    for article_conditional, comment_conditional in [
        ({}, {}),
        (
            {
                "where": {"title": {"_eq": "title"}},
            },
            {
                "where": {"content": {"_eq": "content"}},
            },
        ),
        (
            {"order_by": {"title": "desc"}},
            {"order_by": {"content": "desc"}},
        ),
        (
            {"limit": 2, "order_by": {"title": "desc"}},
            {"limit": 3, "order_by": {"content": "desc"}},
        ),
        (
            {"offset": 3, "order_by": {"title": "desc"}},
            {"offset": 2, "order_by": {"content": "desc"}},
        ),
        (
            {
                "distinct_on": "word_count",
                "order_by": [{"word_count": "asc"}, {"title": "desc"}],
            },
            {
                "distinct_on": "likes",
                "order_by": [{"likes": "desc"}, {"content": "desc"}],
            },
        ),
        (
            {
                "where": {
                    "word_count": {"_lt": 3000},
                },
                "order_by": [{"word_count": "asc"}, {"title": "desc"}],
                "distinct_on": "word_count",
                "limit": 1,
                "offset": 1,
            },
            {
                "where": {
                    "likes": {"_lt": 3},
                },
                "order_by": [{"likes": "desc"}, {"content": "desc"}],
                "distinct_on": "likes",
                "limit": 1,
                "offset": 1,
            },
        ),
    ]
]

COLUMNS_ARG: ParameterizeArgs = {
    "argnames": ["get_columns_arg"],
    "ids": [
        "no columns",
        "author columns",
        "include no columns",
        "include with where condition",
        "include with order_by condition",
        "include with limit condition",
        "include with offset condition",
        "include with distinct_on condition",
        "include with all conditions together",
    ],
    "argvalues": [
        [lambda: {}],
        [
            lambda: {
                "columns": [
                    "uuid",
                    "name",
                    "age",
                    "home_zone",
                ]
            }
        ],
    ]
    + AUTHOR_CONDITIONALS,
}

INPUT_DATA: ParameterizeArgs = {
    "argnames": ["get_input_data"],
    "ids": [
        "simple dict",
        "nested dict",
        "simple model",
        "nested model",
    ],
    "argvalues": [
        [
            lambda user_uuid: {
                "name": "author_1",
                "age": 100,
                "home_zone": Polygon(
                    type="Polygon",
                    coordinates=[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]],
                ),
                "created_by": user_uuid,
                "updated_by": user_uuid,
            }
        ],
        [
            lambda user_uuid: {
                "name": "author_1",
                "age": 100,
                "home_zone": Polygon(
                    type="Polygon",
                    coordinates=[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]],
                ),
                "created_by": user_uuid,
                "updated_by": user_uuid,
                "detail": {
                    "data": {
                        "country": "Canada",
                        "created_by": user_uuid,
                        "updated_by": user_uuid,
                    }
                },
                "articles": {
                    "data": [
                        {
                            "title": f"some title {article_counter + 1}",
                            "word_count": (article_counter % 3 + 1) * 1000,
                            "created_by": user_uuid,
                            "updated_by": user_uuid,
                            "comments": {
                                "data": [
                                    {
                                        "content": (
                                            f"some content {comment_counter + 1}"
                                        ),
                                        "likes": (comment_counter % 3 + 1),
                                        "created_by": user_uuid,
                                        "updated_by": user_uuid,
                                    }
                                    for comment_counter in range(5)
                                ]
                            },
                        }
                        for article_counter in range(5)
                    ]
                },
            }
        ],
        [
            lambda user_uuid: Author(
                name="author_1",
                age=100,
                home_zone=Polygon(
                    type="Polygon",
                    coordinates=[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]],
                ),
                created_by=user_uuid,
                updated_by=user_uuid,
            ).to_hasura_input()
        ],
        [
            lambda user_uuid: Author(
                name="author_1",
                age=100,
                home_zone=Polygon(
                    type="Polygon",
                    coordinates=[[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]],
                ),
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
        ],
    ],
}

INPUT_DATA_LIST: ParameterizeArgs = {
    "argnames": ["get_data_list", "expected_affected_rows"],
    "ids": [
        "list of simple dict",
        "list of nested dict",
        "list of simple model",
        "list of nested model",
    ],
    "argvalues": [
        [
            lambda user_uuid: [
                {
                    "name": "author_1",
                    "age": 50,
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                },
                {
                    "name": "author_2",
                    "age": 100,
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                },
            ],
            2,
        ],
        [
            lambda user_uuid: [
                {
                    "name": "author_1",
                    "age": 50,
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                    "detail": {
                        "data": {
                            "country": "Canada",
                            "created_by": user_uuid,
                            "updated_by": user_uuid,
                        }
                    },
                    "articles": {
                        "data": [
                            {
                                "title": f"some title {article_counter + 1}",
                                "word_count": (article_counter % 3 + 1) * 1000,
                                "created_by": user_uuid,
                                "updated_by": user_uuid,
                                "comments": {
                                    "data": [
                                        {
                                            "content": (
                                                f"some content {comment_counter + 1}"
                                            ),
                                            "likes": (comment_counter % 3 + 1),
                                            "created_by": user_uuid,
                                            "updated_by": user_uuid,
                                        }
                                        for comment_counter in range(5)
                                    ]
                                },
                            }
                            for article_counter in range(5)
                        ]
                    },
                },
                {
                    "name": "author_2",
                    "age": 100,
                    "created_by": user_uuid,
                    "updated_by": user_uuid,
                    "detail": {
                        "data": {
                            "country": "Canada",
                            "created_by": user_uuid,
                            "updated_by": user_uuid,
                        }
                    },
                    "articles": {
                        "data": [
                            {
                                "title": f"some title {article_counter + 1}",
                                "word_count": (article_counter % 3 + 1) * 1000,
                                "created_by": user_uuid,
                                "updated_by": user_uuid,
                                "comments": {
                                    "data": [
                                        {
                                            "content": (
                                                f"some content {comment_counter + 1}"
                                            ),
                                            "likes": (comment_counter % 3 + 1),
                                            "created_by": user_uuid,
                                            "updated_by": user_uuid,
                                        }
                                        for comment_counter in range(5)
                                    ]
                                },
                            }
                            for article_counter in range(5)
                        ]
                    },
                },
            ],
            # 2 recs * (1 author + 1 detail + 5 articles + 25 comments)
            2 * (1 + 1 + 5 + 25),
        ],
        [
            lambda user_uuid: [
                Author(
                    name="author_1",
                    age=50,
                    created_by=user_uuid,
                    updated_by=user_uuid,
                ).to_hasura_input(),
                Author(
                    name="author_2",
                    age=100,
                    created_by=user_uuid,
                    updated_by=user_uuid,
                ).to_hasura_input(),
            ],
            2,
        ],
        [
            lambda user_uuid: [
                Author(
                    name="author_1",
                    age=50,
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
                ).to_hasura_input(),
                Author(
                    name="author_2",
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
                ).to_hasura_input(),
            ],
            # 2 recs * (1 author + 1 detail + 5 articles + 25 comments)
            2 * (1 + 1 + 5 + 25),
        ],
    ],
}
