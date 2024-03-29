from __future__ import annotations

from typing import TYPE_CHECKING, ForwardRef, Optional
from uuid import UUID

from pydantic import BaseModel

from cuckoo.models import (
    AggregateResponse,
    CreatableModel,
    DeletableModel,
    HasuraTableModel,
    IdentityModel,
    UpdatableModel,
)

if TYPE_CHECKING:
    from tests.fixture.sample_models.public.author import Author
    from tests.fixture.sample_models.public.comment import (
        Comment,
        CommentAggregate,
    )
else:
    Author = ForwardRef("Author")
    Comment = ForwardRef("Comment")
    CommentAggregate = ForwardRef("CommentAggregate")


class ArticleNumerics(BaseModel):
    word_count: Optional[float]


class ArticleBase(
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
):
    author_uuid: Optional[UUID]
    title: Optional[str]
    word_count: Optional[int]


class Article(ArticleBase, HasuraTableModel):
    _table_name = "articles"
    author: Optional[Author]
    comments: Optional[list[Comment]]
    comments_aggregate: Optional[CommentAggregate]


ArticleAggregate = AggregateResponse[ArticleBase, ArticleNumerics, Article]
