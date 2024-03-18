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
    from tests.fixture.sample_models.public.article import Article
else:
    Article = ForwardRef("Article")


class CommentBase(
    IdentityModel,
    CreatableModel,
    UpdatableModel,
    DeletableModel,
):
    article_uuid: Optional[UUID]
    content: Optional[str]
    likes: Optional[int]


class CommentNumerics(BaseModel):
    likes: Optional[float]


class Comment(HasuraTableModel, CommentBase):
    _table_name = "comments"
    article: Optional[Article]


CommentAggregate = AggregateResponse[CommentBase, CommentNumerics, Comment]
