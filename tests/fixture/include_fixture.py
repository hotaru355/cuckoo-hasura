from itertools import groupby
from tests.fixture.common_fixture import ParameterizeArgs


ARTICLE_AGGREGATES: ParameterizeArgs = {
    "argnames": ["aggregate_arg", "get_actual", "expected"],
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
                    "columns": {"word_count"},
                    "distinct": True,
                }
            },
            lambda aggr: aggr.count,
            3,
        ),
        (
            {"avg": {"word_count"}},
            lambda aggr: aggr.avg.word_count,
            1900,
        ),
        (
            {"max": {"word_count"}},
            lambda aggr: aggr.max.word_count,
            3000,
        ),
        (
            {"min": {"word_count"}},
            lambda aggr: aggr.min.word_count,
            1000,
        ),
        (
            {"stddev": {"word_count"}},
            lambda aggr: round(aggr.stddev.word_count, 2),
            875.6,
        ),
        (
            {"stddev_pop": {"word_count"}},
            lambda aggr: round(aggr.stddev_pop.word_count, 2),
            830.66,
        ),
        (
            {"stddev_samp": {"word_count"}},
            lambda aggr: round(aggr.stddev_samp.word_count, 2),
            875.6,
        ),
        (
            {"sum": {"word_count"}},
            lambda aggr: aggr.sum.word_count,
            19000,
        ),
        (
            {"var_pop": {"word_count"}},
            lambda aggr: round(aggr.var_pop.word_count, 2),
            690000,
        ),
        (
            {"var_samp": {"word_count"}},
            lambda aggr: round(aggr.var_samp.word_count, 2),
            766666.67,
        ),
        (
            {"variance": {"word_count"}},
            lambda aggr: round(aggr.variance.word_count, 2),
            766666.67,
        ),
        (
            {
                "count": {
                    "columns": {"word_count"},
                    "distinct": True,
                },
                "avg": {"word_count"},
                "max": {"word_count"},
                "min": {"word_count"},
                "stddev": {"word_count"},
                "stddev_pop": {"word_count"},
                "stddev_samp": {"word_count"},
                "sum": {"word_count"},
                "var_pop": {"word_count"},
                "var_samp": {"word_count"},
                "variance": {"word_count"},
            },
            lambda aggr: {
                "count": aggr.count,
                "avg": aggr.avg.word_count,
                "max": aggr.max.word_count,
                "min": aggr.min.word_count,
                "stddev": round(aggr.stddev.word_count, 2),
                "stddev_pop": round(aggr.stddev_pop.word_count, 2),
                "stddev_samp": round(aggr.stddev_samp.word_count, 2),
                "sum": round(aggr.sum.word_count, 2),
                "var_pop": round(aggr.var_pop.word_count, 2),
                "var_samp": round(aggr.var_samp.word_count, 2),
                "variance": round(aggr.variance.word_count, 2),
            },
            {
                "count": 3,
                "avg": 1900.0,
                "max": 3000,
                "min": 1000,
                "stddev": 875.6,
                "stddev_pop": 830.66,
                "stddev_samp": 875.6,
                "sum": 19000,
                "var_pop": 690000.0,
                "var_samp": 766666.67,
                "variance": 766666.67,
            },
        ),
    ],
}
ARTICLE_CONDITIONALS: ParameterizeArgs = {
    "argnames": ["get_article_conditional", "get_expected_articles"],
    "ids": ["where", "order_by", "limit", "offset", "distinct_on"],
    "argvalues": [
        (
            lambda articles: {
                "where": {
                    "title": {"_eq": articles[2].title},
                }
            },
            lambda articles: [articles[2]],
        ),
        (
            lambda articles: {
                "order_by": {"title": "desc"},
            },
            lambda articles: sorted(articles, key=lambda art: art.title, reverse=True),
        ),
        (
            lambda articles: {
                "limit": 2,
                "order_by": {"title": "desc"},
            },
            lambda articles: sorted(
                articles,
                key=lambda art: art.title,
                reverse=True,
            )[:2],
        ),
        (
            lambda articles: {
                "offset": 7,
                "order_by": {"title": "desc"},
            },
            lambda articles: sorted(
                articles,
                key=lambda art: art.title,
                reverse=True,
            )[7:],
        ),
        (
            lambda articles: {
                "distinct_on": "word_count",
                "order_by": [
                    {"word_count": "asc"},
                    {"title": "desc"},
                ],
            },
            lambda articles: [
                next(arts)
                for _, arts in groupby(
                    sorted(
                        sorted(
                            articles,
                            key=lambda art: art.title,
                            reverse=True,
                        ),
                        key=lambda art: art.word_count,
                    ),
                    key=lambda art: art.word_count,
                )
            ],
        ),
    ],
}
SUGAR_FUNCTIONS: ParameterizeArgs = {
    "argnames": [
        "sugar_fn_name",
        "sugar_fn_args",
        "get_actual",
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
        [
            "count",
            {},
            lambda aggr: aggr.count,
            10,
        ],
        [
            "count",
            {"columns": {"word_count"}},
            lambda aggr: aggr.count,
            10,
        ],
        [
            "count",
            {"columns": {"word_count"}, "distinct": True},
            lambda aggr: aggr.count,
            3,
        ],
        [
            "avg",
            {"columns": {"word_count"}},
            lambda aggr: aggr.avg.word_count,
            1900,
        ],
        [
            "max",
            {"columns": {"word_count"}},
            lambda aggr: aggr.max.word_count,
            3000,
        ],
        [
            "min",
            {"columns": {"word_count"}},
            lambda aggr: aggr.min.word_count,
            1000,
        ],
        [
            "sum",
            {"columns": {"word_count"}},
            lambda aggr: aggr.sum.word_count,
            19000,
        ],
    ],
}
