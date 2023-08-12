from tests.fixture.sample_models import Article, Author, AuthorDetail, Comment


class TestHasuraTableModel:
    def test_to_hasura_input(self):
        expected = {
            "name": "test",
            "detail": {
                "data": {
                    "detail": "detail 1",
                }
            },
            "articles": {
                "data": [
                    {
                        "title": "title 1",
                        "comments": {
                            "data": [
                                {
                                    "comment": "comment 1-1",
                                },
                                {
                                    "comment": "comment 1-2",
                                },
                            ]
                        },
                    },
                    {
                        "title": "title 2",
                        "comments": {
                            "data": [
                                {
                                    "comment": "comment 2-1",
                                },
                                {
                                    "comment": "comment 2-2",
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
                detail="detail 1",
            ),
            articles=[
                Article(
                    title=f"title {article_idx}",
                    comments=[
                        Comment(comment=f"comment {article_idx}-{comment_idx}")
                        for comment_idx in range(1, 3)
                    ],
                )
                for article_idx in range(1, 3)
            ],
        ).to_hasura_input()

        assert actual == expected, f"{actual}{expected}"
