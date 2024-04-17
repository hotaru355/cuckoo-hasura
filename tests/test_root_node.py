import time
from statistics import mean
from typing import Any, Callable, Type
from unittest.mock import MagicMock, patch
from uuid import uuid4

from httpx import AsyncClient, Client
from pytest import fixture, mark, raises
from tenacity import stop_after_attempt

from cuckoo import Query, root_node
from cuckoo.errors import HasuraServerError
from cuckoo.include import Include
from tests.fixture.base_fixture import VARIABLE_SEQUENCES, VARIABLE_TYPES
from tests.fixture.common_utils import generate_author_data
from tests.fixture.sample_models.public import Article, Author


class TestJsonifyVariables:
    @mark.parametrize(**VARIABLE_SEQUENCES)
    @mark.parametrize(**VARIABLE_TYPES)
    def test_converts_python_dict_to_json(
        self,
        get_variables_seq: Callable[[list, bool], Any],
        get_expected_seq: Callable[[list], Any],
        variable_values: list,
        expected_values: list,
        is_hashable: bool,
    ):
        expected = {
            "level_1": get_expected_seq(expected_values),
            "nested_1": {
                "level_2": get_expected_seq(expected_values),
                "nested_2": {
                    "level_3": get_expected_seq(expected_values),
                },
            },
        }

        actual = Query._jsonify_variables(
            {
                "level_1": get_variables_seq(variable_values, is_hashable),
                "nested_1": {
                    "level_2": get_variables_seq(variable_values, is_hashable),
                    "nested_2": {
                        "level_3": get_variables_seq(variable_values, is_hashable),
                    },
                },
            }
        )

        assert actual == expected

    @mark.parametrize(**VARIABLE_SEQUENCES)
    @mark.parametrize(**VARIABLE_TYPES)
    def test_converts_python_non_dict_to_json(
        self,
        get_variables_seq: Callable[[list, bool], Any],
        get_expected_seq: Callable[[list], Any],
        variable_values: list,
        expected_values: list,
        is_hashable: bool,
    ):
        expected = get_expected_seq(expected_values)

        actual = Query._jsonify_variables(
            get_variables_seq(variable_values, is_hashable)
        )

        assert actual == expected

    @mark.slow
    @mark.performance
    @mark.parametrize(
        argnames=["num_authors", "num_articles", "num_comments", "limit_seconds"],
        argvalues=[
            [10000, 0, 0, 0.1],
            [1, 100, 1000, 0.1],
        ],
        ids=[
            "flat",
            "deep",
        ],
    )
    def test_performance(self, num_authors, num_articles, num_comments, limit_seconds):
        SAMPLE_SIZE = 10
        seconds_elapsed: list[float] = []
        author_data = generate_author_data(
            num_authors=num_authors,
            num_articles=num_articles,
            num_comments=num_comments,
            user_uuid=uuid4(),
        )

        for _ in range(SAMPLE_SIZE):
            start_time = time.time()
            Query._jsonify_variables(author_data)
            seconds_elapsed.append(time.time() - start_time)

        assert limit_seconds > mean(seconds_elapsed)


class TestIsRoot:
    def test_simple_query_is_root_node(self):
        query = Query(Author)

        query.many(where={}).returning()

        assert query._is_root is True
        assert query._root == query
        assert query._parent == query

    def test_included_subquery_is_not_root_node(self):
        query = Query(Author)
        include = Include(Article)

        query.many(where={}).returning(columns=[include.many().returning()])

        assert include._is_root is False
        assert include._root != include
        assert include._parent != include

    def test_batch_creates_root_node(self):
        with Query.batch() as BatchQuery:
            batch_query = BatchQuery(Author)
            batch_query.many(where={}).yielding()
        root = batch_query._parent

        assert root._is_root is True
        assert root._root == root
        assert root._parent == root

    def test_batch_query_is_not_root_node(self):
        with Query.batch() as BatchQuery:
            batch_query = BatchQuery(Author)
            batch_query.many(where={}).yielding()

        assert batch_query._is_root is False
        assert batch_query._root != batch_query
        assert batch_query._parent != batch_query


class TestExecute:
    def test_default_config_stops_retrying_after_5_attempts(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            Query(Author, session=failing_session).many(where={}).returning()

        assert failing_session.send.call_count == 5

    def test_retry_config_can_be_changed(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            Query(
                Author,
                session=failing_session,
                config={
                    "retry": {
                        "stop": stop_after_attempt(3),
                    }
                },
            ).many(where={}).returning()

        assert failing_session.send.call_count == 3

    def test_default_session_gets_closed_for_successful_query(
        self,
        default_session_close: MagicMock,
    ):
        Query(Author).many(where={}).returning()

        assert default_session_close.call_count == 1

    def test_default_session_gets_closed_for_unsuccessful_query(
        self,
        failing_default_session: MagicMock,
    ):
        with raises(HasuraServerError):
            Query(Author).many(where={}).returning()

        failing_default_session.close.assert_called_once()

    def test_non_default_session_does_not_get_closed_for_successful_query(
        self,
        session: Client,
    ):
        Query(Author, session=session).many(where={}).returning()

        assert not session.is_closed

    def test_non_default_session_does_not_get_closed_for_unsuccessful_query(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            Query(Author, session=failing_session).many(where={}).returning()

        failing_session.close.assert_not_called()

    @fixture
    def failing_session(self):
        mock = MagicMock(spec=Client)
        mock.send.side_effect = [
            Exception("1"),
            Exception("2"),
            Exception("3"),
            Exception("4"),
            Exception("5"),
        ]
        return mock

    @fixture
    def failing_default_session(self, failing_session: MagicMock):
        with patch("cuckoo.root_node.Client", spec=Type[Client]) as mock:
            mock.return_value = failing_session
            yield failing_session

    @fixture
    def default_session_close(self):
        with patch.object(
            root_node.Client,
            "close",
            wraps=root_node.Client().close,
        ) as spy:
            yield spy


@mark.asyncio(scope="session")
class TestExecuteAsync:
    async def test_default_config_stops_retrying_after_5_attempts(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            await (
                Query(Author, session_async=failing_session)
                .many(where={})
                .returning_async()
            )

        assert failing_session.send.call_count == 5

    async def test_retry_config_can_be_changed(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            await (
                Query(
                    Author,
                    session_async=failing_session,
                    config={
                        "retry": {
                            "stop": stop_after_attempt(3),
                        }
                    },
                )
                .many(where={})
                .returning_async()
            )

        assert failing_session.send.call_count == 3

    async def test_default_session_gets_closed_for_successful_query(
        self,
        default_session_close: MagicMock,
    ):
        await Query(Author).many(where={}).returning_async()

        assert default_session_close.call_count == 1

    async def test_default_session_gets_closed_for_unsuccessful_query(
        self,
        failing_default_session: MagicMock,
    ):
        with raises(HasuraServerError):
            await Query(Author).many(where={}).returning_async()

        failing_default_session.aclose.assert_called_once()

    async def test_non_default_session_does_not_get_closed_for_successful_query(
        self,
        session_async: AsyncClient,
    ):
        await (
            Query(Author, session_async=session_async).many(where={}).returning_async()
        )

        assert not session_async.is_closed

    async def test_non_default_session_does_not_get_closed_for_unsuccessful_query(
        self,
        failing_session: MagicMock,
    ):
        with raises(HasuraServerError):
            await (
                Query(Author, session_async=failing_session)
                .many(where={})
                .returning_async()
            )

        failing_session.aclose.assert_not_called()

    @fixture
    def failing_session(self):
        mock = MagicMock(spec=AsyncClient)
        mock.send.side_effect = [
            Exception("1"),
            Exception("2"),
            Exception("3"),
            Exception("4"),
            Exception("5"),
        ]
        return mock

    @fixture
    def failing_default_session(self, failing_session: MagicMock):
        with patch("cuckoo.root_node.AsyncClient", spec=Type[AsyncClient]) as mock:
            mock.return_value = failing_session
            yield failing_session

    @fixture
    def default_session_close(self):
        with patch.object(
            root_node.AsyncClient,
            "aclose",
            wraps=root_node.AsyncClient().aclose,
        ) as spy:
            yield spy
