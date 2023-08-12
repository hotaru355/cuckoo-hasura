from tests.fixture.sample_models.public.address import Address
from tests.fixture.sample_models.public.article import (
    Article,
    ArticleAggregate,
    ArticleBase,
    ArticleNumerics,
)
from tests.fixture.sample_models.public.author_detail import (
    AuthorDetail,
)
from tests.fixture.sample_models.public.author import (
    Author,
    AuthorAggregate,
    AuthorBase,
    AuthorNumerics,
)
from tests.fixture.sample_models.public.comment import (
    Comment,
    CommentAggregate,
    CommentBase,
    CommentNumerics,
)
from tests.fixture.sample_models.public.details_addresses import (
    DetailsAddresses,
)

Article.update_forward_refs(
    Author=Author,
    Comment=Comment,
    CommentAggregate=CommentAggregate,
)
Author.update_forward_refs(
    Article=Article,
    ArticleAggregate=ArticleAggregate,
    AuthorDetail=AuthorDetail,
)
AuthorDetail.update_forward_refs(
    Author=Author,
    Address=Address,
    DetailsAddresses=DetailsAddresses,
)
Comment.update_forward_refs(
    Article=Article,
)
DetailsAddresses.update_forward_refs(
    AuthorDetail=AuthorDetail,
    Address=Address,
)
