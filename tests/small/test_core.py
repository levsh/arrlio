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


class TestApp:
    @pytest.mark.asyncio
    async def test_init(self, cleanup):
        config = Config()
        app = App(config)
        assert app.config == config
        assert isinstance(app.backend, backends.local.Backend)
        assert isinstance(app.backend.serializer, serializers.nop.Serializer)
        await app.close()

    @pytest.mark.asyncio
    async def test_send_task(self, cleanup):
        config = Config()
        app = App(config)

        with mock.patch.object(app.backend, "send_task") as mock_send_task:
            ar = await app.send_task("foo")
            mock_send_task.assert_awaited_once()
            task_instance = mock_send_task.call_args[0][0]

            assert task_instance.task.func is None
            assert task_instance.task.name == "foo"
            assert task_instance.task.bind == settings.TASK_BIND
            assert task_instance.task.queue == settings.TASK_QUEUE
            assert task_instance.task.priority == settings.TASK_PRIORITY
            assert task_instance.task.timeout == settings.TASK_TIMEOUT
            assert task_instance.task.ttl == settings.TASK_TTL
            assert task_instance.task.ack_late == settings.TASK_ACK_LATE
            assert task_instance.task.result_ttl == settings.TASK_RESULT_TTL
            assert task_instance.task.result_return == settings.TASK_RESULT_RETURN
            assert task_instance.task.thread is None

            assert task_instance.data.args == ()
            assert task_instance.data.kwds == {}
            assert task_instance.data.queue == task_instance.task.queue
            assert task_instance.data.priority == task_instance.task.priority
            assert task_instance.data.timeout == task_instance.task.timeout
            assert task_instance.data.ttl == task_instance.task.ttl
            assert task_instance.data.ack_late == task_instance.task.ack_late
            assert task_instance.data.result_ttl == task_instance.task.result_ttl
            assert task_instance.data.result_return == task_instance.task.result_return
            assert task_instance.data.thread is None

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

            assert task_instance.task.func is None
            assert task_instance.task.name == "bar"
            assert task_instance.task.bind == settings.TASK_BIND
            assert task_instance.task.queue == settings.TASK_QUEUE
            assert task_instance.task.priority == settings.TASK_PRIORITY
            assert task_instance.task.timeout == settings.TASK_TIMEOUT
            assert task_instance.task.ttl == settings.TASK_TTL
            assert task_instance.task.ack_late == settings.TASK_ACK_LATE
            assert task_instance.task.result_ttl == settings.TASK_RESULT_TTL
            assert task_instance.task.result_return == settings.TASK_RESULT_RETURN
            assert task_instance.task.thread is None

            assert task_instance.data.args == (1, 2)
            assert task_instance.data.kwds == {"a": "b"}
            assert task_instance.data.queue == "custom"
            assert task_instance.data.priority == 5
            assert task_instance.data.timeout == 999
            assert task_instance.data.ttl == 333
            assert task_instance.data.ack_late is True
            assert task_instance.data.result_ttl == 777
            assert task_instance.data.result_return is False
            assert task_instance.data.thread is True

            assert isinstance(ar, AsyncResult)
            assert ar.task_instance == task_instance

        await app.close()
