from enum import Enum

from cuckoo.models import AggregateResponse
from tests.fixture.common_fixture import (
    ParameterizeArgs,
)
from tests.fixture.common_utils import (
    DEFAULT_COUNTS,
    assert_authors_ordered,
    assert_authors_unordered,
)
from tests.fixture.sample_models.public import Article, Comment

SUGAR_FUNCTIONS: ParameterizeArgs = {
    "argnames": [
        "fn_name",
        "args",
        "get_value",
        "expected",
    ],
    "ids": [
        "count empty",
        "count columns",
        "count columns distinct",
        "avg",
        "max",
        "min",
        "sum",
    ],
    "argvalues": [
        ["count", {}, lambda actual: actual, 10],
        ["count", {"columns": {"age"}}, lambda actual: actual, 10],
        ["count", {"columns": {"age"}, "distinct": True}, lambda actual: actual, 4],
        ["avg", {"columns": {"age"}}, lambda actual: actual.age, 43],
        ["max", {"columns": {"age"}}, lambda actual: actual.age, 60],
        ["min", {"columns": {"age"}}, lambda actual: actual.age, 30],
        ["sum", {"columns": {"age"}}, lambda actual: actual.age, 430],
    ],
}


AUTHOR_CONDITIONALS: ParameterizeArgs = {
    "argnames": ["get_author_conditional", "get_expected_authors"],
    "ids": ["where", "order_by", "limit", "offset", "distinct_on"],
    "argvalues": [
        (
            lambda authors: {
                "where": {
                    "name": {"_eq": authors[2].name},
                }
            },
            lambda authors: [authors[2]],
        ),
        (
            lambda authors: {
                "order_by": {"name": "desc"},
            },
            lambda authors: list(reversed(authors)),
        ),
        (
            lambda authors: {
                "limit": 2,
                "order_by": {"name": "desc"},
            },
            lambda authors: [
                authors[9],
                authors[8],
            ],
        ),
        (
            lambda authors: {
                "offset": 7,
                "order_by": {"name": "desc"},
            },
            lambda authors: [
                authors[2],
                authors[1],
                authors[0],
            ],
        ),
        (
            lambda authors: {
                "distinct_on": "age",
                "order_by": [
                    {"age": "asc"},
                    {"name": "desc"},
                ],
            },
            lambda authors: [
                authors[8],
                authors[9],
                authors[6],
                authors[7],
            ],
        ),
    ],
}


class TestConstant(dict, Enum):
    Author = {"offset": 9}
    Article = {"offset": 3}
    Comment = {"offset": 2}


AUTHOR_ARTICLE_COMMENT_CONDITIONALS: ParameterizeArgs = {
    "argnames": [
        "get_author_condition",
        "get_article_conditional",
        "get_comment_conditional",
        "get_expected_authors",
        "assert_authors",
    ],
    "ids": [
        "NONE",
        "where",
        "order_by",
        "limit",
        "offset",
        "distinct_on",
    ],
    "argvalues": [
        (
            lambda authors: {},
            lambda authors: {},
            lambda authors: {},
            lambda authors: [
                author.copy(
                    update={
                        "articles": [
                            article.copy(
                                update={
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": len(article.comments)}
                                    ),
                                }
                            )
                            for article in author.articles
                        ],
                        "articles_aggregate": AggregateResponse(
                            aggregate={"count": len(author.articles)}
                        ),
                    }
                )
                for author in authors
            ],
            assert_authors_unordered,
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
            assert_authors_unordered,
        ),
        (
            lambda authors: {
                "order_by": {"name": "desc"},
            },
            lambda authors: {
                "order_by": {"title": "desc"},
            },
            lambda authors: {
                "order_by": {"content": "desc"},
            },
            lambda authors: sorted(
                [
                    author.copy(
                        update={
                            "articles": [
                                article.copy(
                                    update={
                                        "comments": [
                                            comment.copy()
                                            for comment in sorted(
                                                article.comments,
                                                key=lambda comment: comment.content,
                                                reverse=True,
                                            )
                                        ],
                                        "comments_aggregate": AggregateResponse(
                                            aggregate={"count": DEFAULT_COUNTS[Comment]}
                                        ),
                                    }
                                )
                                for article in sorted(
                                    author.articles,
                                    key=lambda article: article.title,
                                    reverse=True,
                                )
                            ],
                            "articles_aggregate": AggregateResponse(
                                aggregate={"count": DEFAULT_COUNTS[Article]}
                            ),
                        }
                    )
                    for author in authors
                ],
                key=lambda author: author.name,
                reverse=True,
            ),
            assert_authors_ordered,
        ),
        (
            lambda authors: {
                "limit": 2,
                "order_by": {"name": "desc"},
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
                authors[8].copy(
                    update={
                        "articles": [
                            authors[8]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[8].articles[4].comments[4].copy(),
                                        authors[8].articles[4].comments[3].copy(),
                                        authors[8].articles[4].comments[2].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[8]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[8].articles[3].comments[4].copy(),
                                        authors[8].articles[3].comments[3].copy(),
                                        authors[8].articles[3].comments[2].copy(),
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
            assert_authors_ordered,
        ),
        (
            lambda authors: {
                "offset": TestConstant.Author["offset"],
                "order_by": {"name": "desc"},
            },
            lambda authors: {
                "offset": TestConstant.Article["offset"],
                "order_by": {"title": "desc"},
            },
            lambda authors: {
                "offset": TestConstant.Comment["offset"],
                "order_by": {"content": "desc"},
            },
            lambda authors: sorted(
                [
                    author.copy(
                        update={
                            "articles": [
                                article.copy(
                                    update={
                                        "comments": [
                                            comment.copy()
                                            for comment in sorted(
                                                article.comments,
                                                key=lambda comment: comment.content,
                                                reverse=True,
                                            )[TestConstant.Comment["offset"] :]
                                        ],
                                        "comments_aggregate": AggregateResponse(
                                            aggregate={
                                                "count": len(
                                                    article.comments[
                                                        TestConstant.Comment["offset"] :
                                                    ]
                                                )
                                            }
                                        ),
                                    }
                                )
                                for article in sorted(
                                    author.articles,
                                    key=lambda article: article.title,
                                    reverse=True,
                                )[TestConstant.Article["offset"] :]
                            ],
                            "articles_aggregate": AggregateResponse(
                                aggregate={"count": len(author.articles[3:])}
                            ),
                        }
                    )
                    for author in authors
                ],
                key=lambda author: author.name,
                reverse=True,
            )[TestConstant.Author["offset"] :],
            assert_authors_ordered,
        ),
        (
            lambda authors: {
                "distinct_on": "age",
                "order_by": [
                    {"age": "asc"},
                    {"name": "desc"},
                ],
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
                authors[9].copy(
                    update={
                        "articles": [
                            authors[9]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[9].articles[3].comments[2].copy(),
                                        authors[9].articles[3].comments[4].copy(),
                                        authors[9].articles[3].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[9]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[9].articles[4].comments[2].copy(),
                                        authors[9].articles[4].comments[4].copy(),
                                        authors[9].articles[4].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[9]
                            .articles[2]
                            .copy(
                                update={
                                    "comments": [
                                        authors[9].articles[2].comments[2].copy(),
                                        authors[9].articles[2].comments[4].copy(),
                                        authors[9].articles[2].comments[3].copy(),
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
                authors[6].copy(
                    update={
                        "articles": [
                            authors[6]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[6].articles[3].comments[2].copy(),
                                        authors[6].articles[3].comments[4].copy(),
                                        authors[6].articles[3].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[6]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[6].articles[4].comments[2].copy(),
                                        authors[6].articles[4].comments[4].copy(),
                                        authors[6].articles[4].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[6]
                            .articles[2]
                            .copy(
                                update={
                                    "comments": [
                                        authors[6].articles[2].comments[2].copy(),
                                        authors[6].articles[2].comments[4].copy(),
                                        authors[6].articles[2].comments[3].copy(),
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
                authors[7].copy(
                    update={
                        "articles": [
                            authors[7]
                            .articles[3]
                            .copy(
                                update={
                                    "comments": [
                                        authors[7].articles[3].comments[2].copy(),
                                        authors[7].articles[3].comments[4].copy(),
                                        authors[7].articles[3].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[7]
                            .articles[4]
                            .copy(
                                update={
                                    "comments": [
                                        authors[7].articles[4].comments[2].copy(),
                                        authors[7].articles[4].comments[4].copy(),
                                        authors[7].articles[4].comments[3].copy(),
                                    ],
                                    "comments_aggregate": AggregateResponse(
                                        aggregate={"count": 3}
                                    ),
                                }
                            ),
                            authors[7]
                            .articles[2]
                            .copy(
                                update={
                                    "comments": [
                                        authors[7].articles[2].comments[2].copy(),
                                        authors[7].articles[2].comments[4].copy(),
                                        authors[7].articles[2].comments[3].copy(),
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
            assert_authors_ordered,
        ),
    ],
}

AUTHOR_AGGREGATES: ParameterizeArgs = {
    "argnames": ["aggregate_arg", "get_value", "expected"],
    "ids": [
        "count",
        "count distinct",
        "avg",
        "max",
        "min",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "sum",
        "var_pop",
        "var_samp",
        "variance",
        "ALL",
    ],
    "argvalues": [
        (
            {"count": True},
            lambda aggr: aggr.count,
            10,
        ),
        (
            {
                "count": {
                    "columns": {"age"},
                    "distinct": True,
                }
            },
            lambda aggr: aggr.count,
            4,
        ),
        (
            {"avg": {"age"}},
            lambda aggr: aggr.avg.age,
            43,
        ),
        (
            {"max": {"age"}},
            lambda aggr: aggr.max.age,
            60,
        ),
        (
            {"min": {"age"}},
            lambda aggr: aggr.min.age,
            30,
        ),
        (
            {"stddev": {"age"}},
            lambda aggr: round(aggr.stddev.age, 2),
            11.6,
        ),
        (
            {"stddev_pop": {"age"}},
            lambda aggr: round(aggr.stddev_pop.age, 2),
            11.0,
        ),
        (
            {"stddev_samp": {"age"}},
            lambda aggr: round(aggr.stddev_samp.age, 2),
            11.6,
        ),
        (
            {"sum": {"age"}},
            lambda aggr: aggr.sum.age,
            430,
        ),
        (
            {"var_pop": {"age"}},
            lambda aggr: round(aggr.var_pop.age, 2),
            121.0,
        ),
        (
            {"var_samp": {"age"}},
            lambda aggr: round(aggr.var_samp.age, 2),
            134.44,
        ),
        (
            {"variance": {"age"}},
            lambda aggr: round(aggr.variance.age, 2),
            134.44,
        ),
        (
            {
                "count": {
                    "columns": {"age"},
                    "distinct": True,
                },
                "avg": {"age"},
                "max": {"age"},
                "min": {"age"},
                "stddev": {"age"},
                "stddev_pop": {"age"},
                "stddev_samp": {"age"},
                "sum": {"age"},
                "var_pop": {"age"},
                "var_samp": {"age"},
                "variance": {"age"},
            },
            lambda aggr: {
                "count": aggr.count,
                "avg": aggr.avg.age,
                "max": aggr.max.age,
                "min": aggr.min.age,
                "stddev": round(aggr.stddev.age, 2),
                "stddev_pop": round(aggr.stddev_pop.age, 2),
                "stddev_samp": round(aggr.stddev_samp.age, 2),
                "sum": round(aggr.sum.age, 2),
                "var_pop": round(aggr.var_pop.age, 2),
                "var_samp": round(aggr.var_samp.age, 2),
                "variance": round(aggr.variance.age, 2),
            },
            {
                "count": 4,
                "avg": 43,
                "max": 60,
                "min": 30,
                "stddev": 11.6,
                "stddev_pop": 11.0,
                "stddev_samp": 11.6,
                "sum": 430,
                "var_pop": 121.0,
                "var_samp": 134.44,
                "variance": 134.44,
            },
        ),
    ],
}
