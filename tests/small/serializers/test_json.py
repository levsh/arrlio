import datetime
import sys
from uuid import UUID

import pydantic

import arrlio
from arrlio import serializers
from arrlio.models import Event, Task, TaskResult


class TestSerializer:
    def test__init(self):
        serializers.json.Serializer(serializers.json.Config())

    def test_dumps_task_instance(self):
        serializer = serializers.json.Serializer(serializers.json.Config())
        task_instance = Task(None, "test").instantiate(task_id="2d29459b-3245-492e-977b-09043c0f1f27", queue="queue")
        assert serializer.dumps_task_instance(task_instance) == (
            b'{"name": "test", "queue": "queue", "priority": 1, "timeout": 300, "ttl": 300, '
            b'"ack_late": false, "result_ttl": 300, "result_return": true, "events": false, "event_ttl": 300, '
            b'"extra": {}, "task_id": "2d29459b-3245-492e-977b-09043c0f1f27", "args": [], "kwds": {}, "meta": {}}'
        )

    def test_loads_task_instance(self):
        class M(pydantic.BaseModel):
            x: int

        @arrlio.task(name="86e68", loads=lambda data: ((M(**data),), {}))
        def foo(m: M):
            pass

        serializer = serializers.json.Serializer(serializers.json.Config())
        task_instance = serializer.loads_task_instance(
            (
                b'{"name": "86e68", "queue": "queue", "priority": 1, "timeout": 300, "ttl": 300, '
                b'"ack_late": false, "result_ttl": 300, "result_return": true, "events": false, "event_ttl": 300, '
                b'"extra": {}, "task_id": "2d29459b-3245-492e-977b-09043c0f1f27", "args": [{"x": 1}], "kwds": {}, '
                b'"meta": {}}'
            )
        )
        assert task_instance == arrlio.registered_tasks["86e68"].instantiate(
            task_id="2d29459b-3245-492e-977b-09043c0f1f27",
            queue="queue",
            args=(M(x=1),),
        )
        assert isinstance(task_instance.args[0], M)

    def test_dumps_task_result(self):
        class M(pydantic.BaseModel):
            x: int

        @arrlio.task(name="36fc32", dumps=lambda res: res.model_dump())
        def foo():
            return M(x=1)

        serializer = serializers.json.Serializer(serializers.json.Config())

        task_instance = arrlio.registered_tasks["36fc32"].instantiate(
            task_id="2d29459b-3245-492e-977b-09043c0f1f27",
            queue="queue",
        )
        task_result = TaskResult(res=foo())
        assert (
            serializer.dumps_task_result(task_result, task_instance)
            == b'{"res": {"x": 1}, "exc": null, "trb": null, "idx": null, "routes": null}'
        )

        try:
            1 / 0
        except ZeroDivisionError:
            exc_info = sys.exc_info()
            exc = exc_info[1]
            trb = exc_info[2]

        task_result = TaskResult(exc=exc, trb=trb)
        if sys.version_info.minor < 11:
            assert serializer.dumps_task_result(task_result, task_instance) == (
                b'{"res": null, "exc": ["builtins", "ZeroDivisionError", "division by zero"], '
                b'"trb": "  File \\"%s\\", line 70, '
                b'in test_dumps_task_result\\n    1 / 0\\n", "idx": null, "routes": null}' % __file__.encode()
            )
        else:
            assert serializer.dumps_task_result(task_result, task_instance) == (
                b'{"res": null, "exc": ["builtins", "ZeroDivisionError", "division by zero"], '
                b'"trb": "  File \\"%s\\", line 70, '
                b'in test_dumps_task_result\\n    1 / 0\\n    ~~^~~\\n", "idx": null, "routes": null}'
                % __file__.encode()
            )

    def test_loads_task_result(self):
        serializer = serializers.json.Serializer(serializers.json.Config())

        assert serializer.loads_task_result(
            b'{"res": "ABC", "exc": null, "idx": null, "trb": null, "routes": null}'
        ) == TaskResult(res="ABC")

        result = serializer.loads_task_result(
            (
                b'{"res": null, "exc": ["builtins", "ZeroDivisionError", "division by zero"], '
                b'"trb": "  File \\"%s\\", line 41, in '
                b'test_dumps_task_result\\n    1 / 0\\n", "idx": null, "routes": null}' % __file__.encode()
            )
        )
        assert isinstance(result, TaskResult)
        assert result.res is None
        assert isinstance(result.exc, ZeroDivisionError)
        assert result.trb == '  File "%s", line 41, in test_dumps_task_result\n    1 / 0\n' % __file__

    def test_dumps_event(self):
        serializer = serializers.json.Serializer(serializers.json.Config())

        event = Event(
            event_id="f3410fd3-660c-4e26-b433-a6c2f5bdf700",
            type="TP",
            dt=datetime.datetime(2022, 3, 12),
            data={"k": "v"},
        )
        assert serializer.dumps_event(event) == (
            b'{"type": "TP", "data": {"k": "v"}, "event_id": "f3410fd3-660c-4e26-b433-a6c2f5bdf700", '
            b'"dt": "2022-03-12T00:00:00", "ttl": 300}'
        )

    def test_loads_event(self):
        serializer = serializers.json.Serializer(serializers.json.Config())

        event = serializer.loads_event(
            (
                b'{"type": "TP", "data": {"k": "v"}, "event_id": "f3410fd3-660c-4e26-b433-a6c2f5bdf700", '
                b'"dt": "2022-03-12T00:00:00", "ttl": 300}'
            )
        )
        assert event == Event(
            event_id="f3410fd3-660c-4e26-b433-a6c2f5bdf700",
            type="TP",
            dt=datetime.datetime(2022, 3, 12),
            data={"k": "v"},
        )
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.dt, datetime.datetime)
