from unittest.mock import MagicMock, patch

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises

from cuckoo.constants import HASURA_DEFAULT_CONFIG, HASURA_HEADERS, HASURA_URL
from cuckoo.cuckoo import Cuckoo
from cuckoo.errors import HasuraClientError, HasuraServerError
from cuckoo.query import Query
from tests.fixture.sample_models.public import Author


@fixture
def spy_session():
    session = Client()
    with patch.object(session, "send", wraps=session.send) as spy_send:
        yield session, spy_send


@fixture
def spy_session2():
    session = Client()
    with patch.object(session, "send", wraps=session.send) as spy_send:
        yield session, spy_send


@fixture
def spy_session_async():
    session = AsyncClient()
    with patch.object(session, "send", wraps=session.send) as spy_send:
        yield session, spy_send


@fixture
def spy_session_async2():
    session = AsyncClient()
    with patch.object(session, "send", wraps=session.send) as spy_send:
        yield session, spy_send


@fixture
def clear_env_vars():
    """Configure cuckoo as if it had no Hasura env vars set.
    If the env vars were actually cleared, pre-test DB migrations would fail
    TODO: move migration into container build
    """
    Cuckoo.configure(
        cuckoo_config={
            "headers": None,
            "url": None,
        },  # type: ignore
    )
    # remove setup from any previous tests
    Cuckoo._global_config["session"] = None
    Cuckoo._global_config["session_async"] = None


@fixture
def default_env_vars():
    Cuckoo.configure(
        cuckoo_config=HASURA_DEFAULT_CONFIG,
    )
    # remove setup from any previous tests
    Cuckoo._global_config["session"] = None
    Cuckoo._global_config["session_async"] = None


@mark.asyncio(scope="session")
class TestConfigure:
    @mark.usefixtures("clear_env_vars")
    async def test_missing_url_raises_error(self, session: Client):
        Cuckoo.configure(
            cuckoo_config={
                # URL missing
                "headers": HASURA_DEFAULT_CONFIG["headers"],
            }
        )

        with raises(HasuraClientError, match="Missing Hasura server URL"):
            Query(Author, session=session).many(where={}).returning()

    @mark.skip(
        reason="Currently a bug: errors from Hasura are swallowed in streaming mode"
    )
    @mark.usefixtures("clear_env_vars")
    async def test_missing_headers_for_streamed_query_raises_error(
        self, session: Client
    ):
        Cuckoo.configure(
            cuckoo_config={
                # headers missing
                "url": HASURA_DEFAULT_CONFIG["url"],
            }
        )

        with raises(
            HasuraServerError, match="x-hasura-access-key required, but not found"
        ):
            Query(Author, session=session).many(where={}).returning()

    @mark.usefixtures("clear_env_vars")
    async def test_missing_headers_for_async_query_raises_error(
        self, session_async: AsyncClient
    ):
        Cuckoo.configure(
            cuckoo_config={
                # headers missing
                "url": HASURA_DEFAULT_CONFIG["url"],
            }
        )

        with raises(
            HasuraServerError, match="x-hasura-access-key required, but not found"
        ):
            await (
                Query(Author, session_async=session_async)
                .many(where={})
                .returning_async()
            )

    @mark.usefixtures("default_env_vars")
    async def test_url_and_headers_can_be_set_by_env_vars(self, session: Client):
        # fixture sets env vars

        query = Query(Author, session=session)
        authors = query.many(where={}).returning()

        assert authors == []
        assert Cuckoo.cuckoo_config()["url"] == query._config["url"] == HASURA_URL
        assert (
            Cuckoo.cuckoo_config()["headers"]
            == query._config["headers"]
            == HASURA_HEADERS
        )

    @mark.usefixtures("clear_env_vars")
    async def test_url_and_headers_can_can_be_set_by_configure(self, session):
        assert Cuckoo.cuckoo_config()["url"] is None
        assert Cuckoo.cuckoo_config()["headers"] is None

        Cuckoo.configure(
            cuckoo_config={
                "url": HASURA_URL,
                "headers": HASURA_HEADERS,
            }
        )
        query = Query(Author, session=session)
        authors = query.many(where={}).returning()

        assert authors == []
        assert Cuckoo.cuckoo_config()["url"] == query._config["url"] == HASURA_URL
        assert (
            Cuckoo.cuckoo_config()["headers"]
            == query._config["headers"]
            == HASURA_HEADERS
        )

    @mark.usefixtures("clear_env_vars")
    async def test_url_and_headers_can_can_be_set_by_constructor(self, session: Client):
        assert Cuckoo.cuckoo_config()["url"] is None
        assert Cuckoo.cuckoo_config()["headers"] is None

        query = Query(
            Author,
            session=session,
            config={
                "url": HASURA_URL,
                "headers": HASURA_HEADERS,
            },
        )
        authors = query.many(where={}).returning()

        assert authors == []
        assert (
            Cuckoo.cuckoo_config()["url"] is None
        ), "settting the URL on a query should not set it on the global config"
        assert (
            Cuckoo.cuckoo_config()["headers"] is None
        ), "settting the headers on a query should not set them on the global config"
        assert query._config["url"] == HASURA_URL
        assert query._config["headers"] == HASURA_HEADERS

    @mark.usefixtures("clear_env_vars")
    async def test_url_and_headers_set_by_constructor_take_precedence_over_global_settings(
        self, session: Client
    ):
        Cuckoo.configure(
            cuckoo_config={
                "url": "globalurl",
                "headers": {"test-header": "test"},
            }
        )

        query = Query(
            Author,
            session=session,
            config={
                "url": HASURA_URL,
                "headers": HASURA_HEADERS,
            },
        )
        authors = query.many(where={}).returning()

        assert authors == []
        assert query._config["url"] == HASURA_URL
        assert query._config["headers"] == HASURA_HEADERS

    @mark.usefixtures("default_env_vars")
    async def test_session_can_be_set_by_configure(
        self,
        spy_session: tuple[Client, MagicMock],
    ):
        session, spy_send = spy_session

        Cuckoo.configure(session=session)
        query = Query(Author)
        authors = query.many(where={}).returning()

        spy_send.assert_called_once()
        assert authors == []
        assert query.session == session

    @mark.usefixtures("default_env_vars")
    async def test_async_session_can_be_set_by_configure(
        self,
        spy_session_async: tuple[AsyncClient, MagicMock],
    ):
        session_async, spy_send = spy_session_async

        Cuckoo.configure(session_async=session_async)
        query = Query(Author)
        authors = await query.many(where={}).returning_async()

        spy_send.assert_called_once()
        assert authors == []
        assert query.session_async == session_async

    @mark.usefixtures("default_env_vars")
    async def test_session_can_be_set_by_constructor(
        self,
        spy_session: tuple[Client, MagicMock],
    ):
        session, spy_send = spy_session

        query = Query(Author, session=session)
        authors = query.many(where={}).returning()

        spy_send.assert_called_once()
        assert authors == []
        assert query.session == session

    @mark.usefixtures("default_env_vars")
    async def test_async_session_can_be_set_by_constructor(
        self,
        spy_session_async: tuple[AsyncClient, MagicMock],
    ):
        session_async, spy_send = spy_session_async

        query = Query(Author, session_async=session_async)
        authors = await query.many(where={}).returning_async()

        spy_send.assert_called_once()
        assert authors == []
        assert query.session_async == session_async

    @mark.usefixtures("default_env_vars")
    async def test_session_set_by_constructor_takes_precedence_over_global_session(
        self,
        spy_session: tuple[Client, MagicMock],
        spy_session2: tuple[Client, MagicMock],
    ):
        session_local, spy_send_local = spy_session
        session_global, spy_send_global = spy_session2

        Cuckoo.configure(session=session_global)
        query = Query(Author, session=session_local)
        authors = query.many(where={}).returning()

        spy_send_local.assert_called_once()
        spy_send_global.assert_not_called()
        assert authors == []
        assert query.session == session_local

    @mark.usefixtures("default_env_vars")
    async def test_async_session_set_by_constructor_takes_precedence_over_global_session(
        self,
        spy_session_async: tuple[AsyncClient, MagicMock],
        spy_session_async2: tuple[AsyncClient, MagicMock],
    ):
        session_async_local, spy_send_local = spy_session_async
        session_async_global, spy_send_global = spy_session_async2

        Cuckoo.configure(session_async=session_async_global)
        query = Query(Author, session_async=session_async_local)
        authors = await query.many(where={}).returning_async()

        spy_send_local.assert_called_once()
        spy_send_global.assert_not_called()
        assert authors == []
        assert query.session_async == session_async_local

    @mark.usefixtures("default_env_vars")
    async def test_missing_session_raises_error(self):
        query = Query(Author)

        with raises(HasuraClientError, match="No session provided"):
            query.many(where={}).returning()

        with raises(HasuraClientError, match="No session provided"):
            query.session

    @mark.usefixtures("default_env_vars")
    async def test_missing_async_session_raises_error(self):
        query = Query(Author)

        with raises(HasuraClientError, match="No async session provided"):
            await query.many(where={}).returning_async()

        with raises(HasuraClientError, match="No async session provided"):
            query.session_async
