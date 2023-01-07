import uuid

import pytest

import arrlio
from arrlio import models, settings


class Test_TaskData:
    def test__init_default(self):
        task_data = models.TaskData()
        assert isinstance(task_data.task_id, uuid.UUID)
        assert task_data.args == ()
        assert task_data.kwds == {}
        assert task_data.queue == settings.TASK_QUEUE
        assert task_data.priority == settings.TASK_PRIORITY
        assert task_data.timeout == settings.TASK_TIMEOUT
        assert task_data.ttl == settings.TASK_TTL
        assert task_data.ack_late == settings.TASK_ACK_LATE
        assert task_data.result_ttl == settings.TASK_RESULT_TTL
        assert task_data.result_return == settings.TASK_RESULT_RETURN
        assert task_data.events == settings.TASK_EVENTS
        assert task_data.event_ttl == settings.TASK_EVENT_TTL
        assert task_data.thread is None

    def test__init_custom(self):
        task_data = models.TaskData(
            task_id="cbb6f1e9-2ae7-4674-bce1-83ab4b3d2ce9",
            args=[1, 2],
            kwds={"k": "v"},
            queue="Q",
            priority=777,
            timeout=555,
            ttl=333,
            ack_late=False,
            result_ttl=111,
            result_return=False,
            events=["task done"],
            event_ttl=None,
            thread=True,
        )
        assert isinstance(task_data.task_id, uuid.UUID)
        assert task_data.task_id == uuid.UUID("cbb6f1e9-2ae7-4674-bce1-83ab4b3d2ce9")
        assert task_data.args == (1, 2)
        assert task_data.kwds == {"k": "v"}
        assert task_data.queue == "Q"
        assert task_data.priority == 777
        assert task_data.timeout == 555
        assert task_data.ttl == 333
        assert task_data.ack_late is False
        assert task_data.result_ttl == 111
        assert task_data.result_return is False
        assert task_data.events == ["task done"]
        assert task_data.event_ttl is None
        assert task_data.thread is True


class Test_Task:
    def test__init_default(self):
        def foo():
            pass

        task = models.Task(foo, "foo")
        assert task.func == foo
        assert task.name == "foo"
        assert task.bind == settings.TASK_BIND
        assert task.queue == settings.TASK_QUEUE
        assert task.priority == settings.TASK_PRIORITY
        assert task.timeout == settings.TASK_TIMEOUT
        assert task.ttl == settings.TASK_TTL
        assert task.ack_late == settings.TASK_ACK_LATE
        assert task.result_ttl == settings.TASK_RESULT_TTL
        assert task.result_return == settings.TASK_RESULT_RETURN
        assert task.events == settings.TASK_EVENTS
        assert task.event_ttl == settings.TASK_EVENT_TTL
        assert task.thread is None

        task_instance = task.instantiate()
        assert task_instance.task == task
        assert isinstance(task_instance.data.task_id, uuid.UUID)
        assert task_instance.data.args == ()
        assert task_instance.data.kwds == {}
        assert task_instance.data.queue == task.queue
        assert task_instance.data.priority == task.priority
        assert task_instance.data.timeout == task.timeout
        assert task_instance.data.ttl == task.ttl
        assert task_instance.data.ack_late == task.ack_late
        assert task_instance.data.result_ttl == task.result_ttl
        assert task_instance.data.result_return == task.result_return
        assert task_instance.data.events == task.events
        assert task_instance.data.event_ttl == task.event_ttl
        assert task_instance.data.thread is None

    def test__init_custom(self):
        def foo():
            pass

        task = models.Task(
            foo,
            "foo",
            bind=True,
            queue="Q",
            priority=777,
            timeout=555,
            ttl=333,
            ack_late=False,
            result_ttl=111,
            result_return=False,
            events=True,
            event_ttl=None,
            thread=True,
        )
        assert task.func == foo
        assert task.name == "foo"
        assert task.bind is True
        assert task.queue == "Q"
        assert task.priority == 777
        assert task.timeout == 555
        assert task.ttl == 333
        assert task.ack_late is False
        assert task.result_ttl == 111
        assert task.result_return is False
        assert task.events is True
        assert task.event_ttl is None
        assert task.thread is True

        task_instance = task.instantiate(
            task_id="e67b80b9-a9f0-4ff1-89e8-0beb70993ffd",
            args=[1],
            kwds={"k": "v"},
            queue="QQ",
            priority=7,
            timeout=5,
            ttl=3,
            ack_late=True,
            result_ttl=1,
            result_return=True,
            events=["task done"],
            event_ttl=0,
            thread=False,
        )
        assert task_instance.task == task
        assert task_instance.data.task_id == uuid.UUID("e67b80b9-a9f0-4ff1-89e8-0beb70993ffd")
        assert task_instance.data.args == (1,)
        assert task_instance.data.kwds == {"k": "v"}
        assert task_instance.data.queue == "QQ"
        assert task_instance.data.priority == 7
        assert task_instance.data.timeout == 5
        assert task_instance.data.ttl == 3
        assert task_instance.data.ack_late is True
        assert task_instance.data.result_ttl == 1
        assert task_instance.data.result_return is True
        assert task_instance.data.events == ["task done"]
        assert task_instance.data.event_ttl == 0
        assert task_instance.data.thread is False

    def test_sync(self):
        def foo():
            return "Foo!"

        assert models.Task(foo, "foo").instantiate()() == "Foo!"

    @pytest.mark.asyncio
    async def test_async(self):
        async def bar(*args, **kwds):
            return "Bar!"

        assert await models.Task(bar, "bar").instantiate()() == "Bar!"
        assert await models.Task(bar, "bar", bind=True)() == "Bar!"


@pytest.mark.asyncio
async def test_async_task():
    @arrlio.task
    async def async_task():
        return "Hello from async_task!"

    assert await async_task() == "Hello from async_task!"


def test_sync_task():
    @arrlio.task
    def sync_task():
        return "Hello from sync_task!"

    assert sync_task() == "Hello from sync_task!"


def test_graph():
    @arrlio.task
    def A():
        pass

    @arrlio.task
    def B():
        pass

    @arrlio.task
    def C():
        pass

    graph = models.Graph("Test")
    assert graph.name == "Test"
    assert graph.nodes == {}
    assert graph.edges == {}
    assert graph.roots == set()

    graph.add_node("A", A, root=True)
    graph.add_node("B", B, args=(1,), kwds={"a": "b"})
    graph.add_node("C", C)
    graph.add_node("Z", A)
    assert graph.nodes == {
        "A": ["test_models.A", {}],
        "B": ["test_models.B", {"args": (1,), "kwds": {"a": "b"}}],
        "C": ["test_models.C", {}],
        "Z": ["test_models.A", {}],
    }

    graph.add_edge("A", "B")
    graph.add_edge("B", "B")
    graph.add_edge("A", "C")
    assert graph.edges == {"A": [["B", None], ["C", None]], "B": [["B", None]]}

    assert graph.roots == {"A"}


def test_dict():
    @arrlio.task(
        loads=lambda *args, **kwds: (args, kwds),
        dumps=lambda r: r,
    )
    def foo(x, y=None):
        pass

    task_instance = foo.instantiate(args=(1,), kwds={"y": "value"})

    data = task_instance.task.dict()
    data.pop("func")
    assert data == {
        "name": "test_models.foo",
        "bind": False,
        "queue": "arrlio.tasks",
        "priority": 1,
        "timeout": 300,
        "ttl": 300,
        "ack_late": False,
        "result_ttl": 300,
        "result_return": True,
        "thread": None,
        "events": False,
        "event_ttl": 300,
        "extra": {},
    }

    data = task_instance.data.dict()
    data.pop("task_id")
    assert data == {
        "args": (1,),
        "kwds": {"y": "value"},
        "meta": {},
        "queue": "arrlio.tasks",
        "priority": 1,
        "timeout": 300,
        "ttl": 300,
        "ack_late": False,
        "result_ttl": 300,
        "result_return": True,
        "thread": None,
        "events": False,
        "event_ttl": 300,
        "extra": {},
    }

    data = task_instance.data.dict(exclude=["args", "kwds"])
    data.pop("task_id")
    assert data == {
        "meta": {},
        "queue": "arrlio.tasks",
        "priority": 1,
        "timeout": 300,
        "ttl": 300,
        "ack_late": False,
        "result_ttl": 300,
        "result_return": True,
        "thread": None,
        "events": False,
        "event_ttl": 300,
        "extra": {},
    }

    data = task_instance.dict(exclude=["data.args", "data.kwds"])
    data["task"].pop("func")
    data["data"].pop("task_id")
    assert data == {
        "task": {
            "name": "test_models.foo",
            "bind": False,
            "queue": "arrlio.tasks",
            "priority": 1,
            "timeout": 300,
            "ttl": 300,
            "ack_late": False,
            "result_ttl": 300,
            "result_return": True,
            "thread": None,
            "events": False,
            "event_ttl": 300,
            "extra": {},
        },
        "data": {
            "meta": {},
            "queue": "arrlio.tasks",
            "priority": 1,
            "timeout": 300,
            "ttl": 300,
            "ack_late": False,
            "result_ttl": 300,
            "result_return": True,
            "thread": None,
            "events": False,
            "event_ttl": 300,
            "extra": {},
        },
    }
