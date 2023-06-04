from unittest import mock

import pytest

from arrlio import App, AsyncResult, Config, backends, serializers, settings, task


def test_task():
    @task
    async def foo():
        pass

    @task(bind=True)
    async def bar():
        pass

    with pytest.raises(TypeError):

        @task
        class TaskInvalid:
            pass


class TestApp:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        config = Config()
        app = App(config)
        assert app.config == config
        assert isinstance(app.backend, backends.local.Backend)
        assert isinstance(app.backend._serializer, serializers.nop.Serializer)
        await app.close()

    @pytest.mark.asyncio
    async def test_send_task(self, cleanup):
        config = Config()
        app = App(config)

        with mock.patch.object(app.backend, "send_task") as mock_send_task:
            ar = await app.send_task("foo")
            mock_send_task.assert_awaited_once()
            task_instance = mock_send_task.call_args[0][0]

            assert task_instance.func is None
            assert task_instance.name == "foo"
            assert task_instance.bind == settings.TASK_BIND
            assert task_instance.queue == settings.TASK_QUEUE
            assert task_instance.priority == settings.TASK_PRIORITY
            assert task_instance.timeout == settings.TASK_TIMEOUT
            assert task_instance.ttl == settings.TASK_TTL
            assert task_instance.ack_late == settings.TASK_ACK_LATE
            assert task_instance.args == ()
            assert task_instance.kwds == {}
            assert task_instance.result_ttl == settings.TASK_RESULT_TTL
            assert task_instance.result_return == settings.TASK_RESULT_RETURN
            assert task_instance.thread is None

            assert isinstance(ar, AsyncResult)
            assert ar.task_instance == task_instance

        with mock.patch.object(app.backend, "send_task") as mock_send_task:
            ar = await app.send_task(
                "bar",
                args=(1, 2),
                kwds={"a": "b"},
                queue="custom",
                priority=5,
                timeout=999,
                ttl=333,
                ack_late=True,
                result_ttl=777,
                result_return=False,
                thread=True,
            )
            mock_send_task.assert_awaited_once()
            task_instance = mock_send_task.call_args[0][0]

            assert task_instance.func is None
            assert task_instance.name == "bar"
            assert task_instance.bind == settings.TASK_BIND
            assert task_instance.queue == "custom"
            assert task_instance.priority == 5
            assert task_instance.timeout == 999
            assert task_instance.ttl == 333
            assert task_instance.ack_late is True
            assert task_instance.args == (1, 2)
            assert task_instance.kwds == {"a": "b"}
            assert task_instance.result_ttl == 777
            assert task_instance.result_return is False
            assert task_instance.thread is True

            assert isinstance(ar, AsyncResult)
            assert ar.task_instance == task_instance

        await app.close()

    @pytest.mark.asyncio
    async def test_init(self):
        with mock.patch("arrlio.plugins.events.Plugin.on_init") as mock_events_on_init, mock.patch(
            "arrlio.plugins.graphs.Plugin.on_init"
        ) as mock_graphs_on_init:
            mock_events_on_init.__func__ = True
            mock_graphs_on_init.__func__ = True
            app = App(
                Config(
                    plugins=[
                        {"module": "arrlio.plugins.events"},
                        {"module": "arrlio.plugins.graphs"},
                    ]
                )
            )
            await app.init()
            mock_events_on_init.assert_awaited_once()
            mock_graphs_on_init.assert_awaited_once()
            assert app.plugins["arrlio.events"].name == "arrlio.events"
            assert app.plugins["arrlio.graphs"].name == "arrlio.graphs"
