import re
from typing import Callable
from uuid import UUID

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo import Include, Query
from cuckoo.delete import Delete
from cuckoo.errors import HasuraClientError
from cuckoo.models import Aggregate
from tests.fixture.common_fixture import (
    FinalizeParams,
    FinalizeReturning,
)
from tests.fixture.common_utils import (
    persist_author_details,
    persist_authors,
)
from tests.fixture.include_fixture import (
    ARTICLE_AGGREGATES,
    ARTICLE_CONDITIONALS,
    SUGAR_FUNCTIONS,
)
from tests.fixture.sample_models.public import (
    Address,
    Article,
    Author,
    AuthorDetail,
    Comment,
)


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_one())
class TestOne:
    async def test_can_include_by_key_for_model_with_ambiguous_submodels(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        detail_prim = await finalize(
            run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                uuid=persisted_detail.uuid
            ),
            session=session,
            session_async=session_async,
            columns=[
                Include(Address, field_name="primary_address")
                .one()
                .returning(columns=["street"])
            ],
        )
        detail_sec = await finalize(
            run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                uuid=persisted_detail.uuid
            ),
            session=session,
            session_async=session_async,
            columns=[
                Include(Address, field_name="secondary_address")
                .one()
                .returning(columns=["street"])
            ],
        )

        assert detail_prim.primary_address.street == "primary street"
        assert detail_prim.secondary_address is None
        assert detail_sec.secondary_address.street == "secondary street"
        assert detail_sec.primary_address is None

    async def test_raises_client_error_if_key_to_include_does_not_exist_on_model(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(HasuraClientError) as err:
            await finalize(
                run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                    uuid=persisted_detail.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[Include(Address, field_name="INVALID").one().returning()],
            )

        assert (
            "Invalid sub-query. "
            "The provided `field_name=INVALID` does not exist on model "
            "`<class 'tests.fixture.sample_models.public.author_detail.AuthorDetail'>`"
        ) in str(err)

    async def test_raises_client_error_if_key_to_include_exists_but_model_does_not_match_key(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(HasuraClientError) as err:
            await finalize(
                run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                    uuid=persisted_detail.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[
                    Include(Comment, field_name="primary_address").one().returning()
                ],
            )

        assert (
            "Invalid sub-query. The provided model "
            "`<class 'tests.fixture.sample_models.public.comment.Comment'>` does not "
            "match expected model "
            "`<class 'tests.fixture.sample_models.public.address.Address'>`"
        ) in str(err)

    async def test_raises_client_error_if_included_model_does_not_exist_on_parent_model(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(HasuraClientError) as err:
            await finalize(
                run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                    uuid=persisted_detail.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[Include(Comment).one().returning()],
            )

        assert (
            "Invalid sub-query. Could not find any reference to "
            "<class 'tests.fixture.sample_models.public.comment.Comment'> in "
            "<class 'tests.fixture.sample_models.public.author_detail.AuthorDetail'"
        ) in str(err)

    async def test_raises_client_error_if_no_key_is_provided_for_ambiguous_model(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(HasuraClientError) as err:
            await finalize(
                run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                    uuid=persisted_detail.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[Include(Address).one().returning()],
            )

        match = re.match(
            r".*Ambiguous sub query. Candidates: \['(\w+)', '(\w+)'\]\. "
            r"Use the `field_name` argument to select one\.",
            str(err),
        )
        assert match
        field_names = match.groups()
        assert set(field_names) == {"primary_address", "secondary_address"}

    async def test_raises_error_if_including_invalid_object(
        self,
        finalize: FinalizeReturning[Query, AuthorDetail],
        persisted_detail: AuthorDetail,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(ValueError) as err:
            await finalize(
                run_test=lambda Query: Query(AuthorDetail).one_by_pk(
                    uuid=persisted_detail.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[Include(Address).one()],
            )

        assert (
            "Elements in `returning` need to be of type `str` or an instance of `Include`. "
            "Found type=<class 'cuckoo.finalizers.IncludeFinalizer'>."
        ) in str(err)


@mark.asyncio(scope="session")
@mark.parametrize(**FinalizeParams(Query).returning_one())
class TestAggregate:
    @mark.parametrize(**ARTICLE_AGGREGATES)
    async def test_aggregate_on_without_conditions(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_author: Author,
        aggregate_arg: dict,
        get_actual: Callable[[Aggregate], float],
        expected: float,
        session: Client,
        session_async: AsyncClient,
    ):
        author = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=persisted_author.uuid),
            session=session,
            session_async=session_async,
            columns=[Include(Article).aggregate().on(**aggregate_arg)],
        )

        actual = get_actual(author.articles_aggregate.aggregate)
        assert actual == expected

    async def test_aggregate_on_raises_error_if_no_aggregate_is_provided(
        self,
        finalize: FinalizeReturning[Query, Author],
        persisted_author: Author,
        session: Client,
        session_async: AsyncClient,
    ):
        with raises(ValueError) as err:
            await finalize(
                run_test=lambda Query: Query(Author).one_by_pk(
                    uuid=persisted_author.uuid
                ),
                session=session,
                session_async=session_async,
                columns=[Include(Article).aggregate().on()],
            )

        assert (
            "Missing argument. At least one argument is required: count, avg, max, "
            "min, stddev, stddev_pop, stddev_samp, sum, var_pop, var_samp, variance."
        ) in str(err)

    @mark.parametrize(**ARTICLE_CONDITIONALS)
    async def test_aggregate_count_with_nodes(
        self,
        finalize: FinalizeReturning[Query, Author],
        get_article_conditional: Callable[[list[Article]], dict],
        get_expected_articles: Callable[[list[Article]], list[Article]],
        persisted_author: Author,
        session: Client,
        session_async: AsyncClient,
    ):
        expected_articles = get_expected_articles(persisted_author.articles)

        author = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=persisted_author.uuid),
            session=session,
            session_async=session_async,
            columns=[
                Include(Article)
                .aggregate(**get_article_conditional(persisted_author.articles))
                .with_nodes(
                    aggregates={"count": True},
                    columns=["uuid", "title"],
                )
            ],
        )
        actual_articles = author.articles_aggregate.nodes

        assert len(actual_articles) == len(expected_articles)
        for actual_article, expected_article in zip(actual_articles, expected_articles):
            assert actual_article.uuid == expected_article.uuid

    @mark.parametrize(**SUGAR_FUNCTIONS)
    async def test_syntactic_sugar_functions(
        self,
        finalize: FinalizeReturning[Query, Author],
        sugar_fn_name: str,
        sugar_fn_args: dict,
        get_actual: Callable[[Aggregate], float],
        expected: float,
        persisted_author: Author,
        session: Client,
        session_async: AsyncClient,
    ):
        sugar_function = getattr(Include(Article).aggregate(), sugar_fn_name)

        author = await finalize(
            run_test=lambda Query: Query(Author).one_by_pk(uuid=persisted_author.uuid),
            session=session,
            session_async=session_async,
            columns=[sugar_function(**sugar_fn_args)],
        )
        actual = get_actual(author.articles_aggregate.aggregate)

        assert actual == expected


def test_raises_error_if_instance_is_used_more_than_once(
    persisted_author: Author,
):
    columns = [Include(Article).many().returning()]
    Query(Author).one_by_pk(uuid=persisted_author.uuid).returning(columns)
    with raises(ValueError) as error:
        Query(Author).one_by_pk(uuid=persisted_author.uuid).returning(columns)
    assert (
        "Found an instance `Include(Article)` that was already used in an executed query"
    ) in str(error)


@fixture(scope="module")
def persisted_author(user_uuid: UUID, session: Client):
    Delete(Author, session=session).many(where={}).affected_rows()

    return persist_authors(
        user_uuid=user_uuid,
        num_authors=1,
        num_articles=10,
        num_comments=0,
        session=session,
    )[0]


@fixture(scope="module")
def persisted_detail(user_uuid: UUID, session: Client):
    Delete(AuthorDetail, session=session).many(where={}).affected_rows()

    return persist_author_details(user_uuid=user_uuid, session=session)[0]
