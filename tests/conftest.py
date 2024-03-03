import os
from http import HTTPStatus
from pathlib import Path
from uuid import uuid4

import pytest_asyncio
from dotenv import load_dotenv
from httpx import AsyncClient, Client, HTTPError
from pytest import fixture
from pytest_docker.plugin import Services

load_dotenv(".env.default")
from cuckoo.constants import HASURA_HEALTH_URL, HASURA_URL  # noqa: E402
from tests.hasura_setup_util import (  # noqa: E402
    clear_metadata,
    create_many_relation,
    create_one_relation,
    run_sql_file,
    track_functions,
    track_tables,
)


@fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Change search path for docker-compse.yml from `tests` folder to root folder."""

    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


def is_hasura_responsive():
    try:
        response = Client(timeout=60).get(HASURA_HEALTH_URL)

        return response.status_code == HTTPStatus.OK
    except HTTPError:
        return False


@fixture(scope="session", autouse=True)
def hasura_service(docker_services: Services):
    """Ensure that Hasura service is up and responsive."""

    docker_services.wait_until_responsive(
        timeout=60, pause=5, check=is_hasura_responsive
    )
    return HASURA_URL


@fixture(scope="module")
def user_uuid():
    return uuid4()


@fixture(scope="session")
def session():
    with Client(timeout=30) as test_session:
        yield test_session


@pytest_asyncio.fixture(scope="session")
async def session_async():
    async with AsyncClient(timeout=30) as test_session:
        yield test_session


@fixture(scope="session", autouse=False)
def setup_hasura():
    clear_metadata()
    run_sql_file(Path("tests/fixture/sample_models/public/public.schema.sql"))
    track_tables(
        [
            "public.authors",
            "public.articles",
            "public.comments",
            "public.addresses",
            "public.author_details",
            "public.details_addresses",
        ]
    )
    # Author
    create_one_relation(
        src_table_column="public.authors.uuid",
        dst_table_column="public.author_details.author_uuid",
        relation_name="detail",
        fk_constraint_on_src=False,
    )
    create_one_relation(
        src_table_column="public.author_details.author_uuid",
        dst_table_column="public.authors.uuid",
        relation_name="author",
    )
    create_many_relation(
        src_table_name="public.authors",
        dst_table_column="public.articles.author_uuid",
        relation_name="articles",
    )

    # Author Detail
    create_one_relation(
        src_table_column="public.author_details.primary_address_uuid",
        dst_table_column="public.addresses.uuid",
        relation_name="primary_address",
    )
    create_one_relation(
        src_table_column="public.author_details.secondary_address_uuid",
        dst_table_column="public.addresses.uuid",
        relation_name="secondary_address",
    )
    create_many_relation(
        src_table_name="public.author_details",
        dst_table_column="public.details_addresses.author_detail_uuid",
        relation_name="past_primary_addresses",
    )
    create_many_relation(
        src_table_name="public.author_details",
        dst_table_column="public.details_addresses.author_detail_uuid",
        relation_name="past_secondary_addresses",
    )

    # Articles
    create_one_relation(
        src_table_column="public.articles.author_uuid",
        dst_table_column="public.authors.uuid",
        relation_name="author",
    )
    create_many_relation(
        src_table_name="public.articles",
        dst_table_column="public.comments.article_uuid",
        relation_name="comments",
    )

    # Comments
    create_one_relation(
        src_table_column="public.comments.article_uuid",
        dst_table_column="public.articles.uuid",
        relation_name="article",
    )

    # DetailsAddresses
    create_one_relation(
        src_table_column="public.details_addresses.address_uuid",
        dst_table_column="public.addresses.uuid",
        relation_name="address",
    )
    create_one_relation(
        src_table_column="public.details_addresses.author_detail_uuid",
        dst_table_column="public.autor_details.uuid",
        relation_name="author_detail",
    )

    # Functions
    track_functions(
        [
            "public.find_authors_with_articles",
            "public.find_most_commented_author",
        ]
    )
    track_functions(
        [
            "public.inc_author_age",
            "public.inc_all_authors_age",
        ],
        exposed_as="mutation",
    )
