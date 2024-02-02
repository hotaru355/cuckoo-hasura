from tests.fixture.sample_models import Article, Author, AuthorDetail, Comment


class TestHasuraTableModel:
    def test_to_hasura_input(self):
        expected = {
            "name": "test",
            "detail": {
                "data": {
                    "country": "some country",
                }
            },
            "articles": {
                "data": [
                    {
                        "title": "title 1",
                        "comments": {
                            "data": [
                                {
                                    "content": "comment 1-1",
                                },
                                {
                                    "content": "comment 1-2",
                                },
                            ]
                        },
                    },
                    {
                        "title": "title 2",
                        "comments": {
                            "data": [
                                {
                                    "content": "comment 2-1",
                                },
                                {
                                    "content": "comment 2-2",
                                },
                            ]
                        },
                    },
                ]
            },
        }

        actual = Author(
            name="test",
            detail=AuthorDetail(
                country="some country",
            ),
            articles=[
                Article(
                    title=f"title {article_idx}",
                    comments=[
                        Comment(content=f"comment {article_idx}-{comment_idx}")
                        for comment_idx in range(1, 3)
                    ],
                )
                for article_idx in range(1, 3)
            ],
        ).to_hasura_input()

        assert actual == expected, f"{actual}{expected}"
