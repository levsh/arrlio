import datetime
import sys
from uuid import UUID

from arrlio import serializers
from arrlio.models import Event, Task, TaskResult


class TestSerializer:
    def test_init(self):
        serializers.json.Serializer()

    def test_dumps_task_instance(self):
        serializer = serializers.json.Serializer()
        task_instance = Task(None, "test").instantiate(task_id="2d29459b-3245-492e-977b-09043c0f1f27", queue="queue")
        assert serializer.dumps_task_instance(task_instance) == (
            b'{"name": "test", "task_id": "2d29459b-3245-492e-977b-09043c0f1f27", "args": [], "kwds": {}, '
            b'"meta": {}, "backend_extra": {}, "queue": "queue", "priority": 1, "timeout": 300, "ttl": 300, "ack_late": false, '
            b'"result_ttl": 300, "result_return": true, "events": false, "event_ttl": 300}'
        )

    def test_loads_task_instance(self):
        serializer = serializers.json.Serializer()
        assert serializer.loads_task_instance(
            (
                b'{"name": "test", "task_id": "2d29459b-3245-492e-977b-09043c0f1f27", "args": [], "kwds": {}, '
                b'"meta": {}, "backend_extra": {}, "queue": "queue", "priority": 1, "timeout": 300, "ttl": 300, "ack_late": false, '
                b'"result_ttl": 300, "result_return": true, "events": false, "event_ttl": 300}'
            )
        ) == Task(None, "test").instantiate(task_id="2d29459b-3245-492e-977b-09043c0f1f27", queue="queue")

    def test_dumps_task_result(self):
        serializer = serializers.json.Serializer()

        task_result = TaskResult(res="ABC")
        assert serializer.dumps_task_result(task_result) == b'["ABC", null, null]'

        try:
            1 / 0
        except ZeroDivisionError:
            exc_info = sys.exc_info()
            exc = exc_info[1]
            trb = exc_info[2]

        task_result = TaskResult(exc=exc, trb=trb)
        assert serializer.dumps_task_result(task_result) == (
            b'[null, ["builtins", "ZeroDivisionError", "division by zero"], "  '
            b'File \\"%s\\", line 39, in '
            b'test_dumps_task_result\\n    1 / 0\\n"]' % __file__.encode()
        )

    def test_loads_task_result(self):
        serializer = serializers.json.Serializer()

        assert serializer.loads_task_result(b'["ABC", null, null]') == TaskResult(res="ABC")

        assert serializer.loads_task_result(
            (
                b'[null, ["builtins", "ZeroDivisionError", "division by zero"], "  '
                b'File \\"%s\\", line 41, in '
                b'test_dumps_task_result\\n    1 / 0\\n"]' % __file__.encode()
            )
        ) == TaskResult(
            exc=["builtins", "ZeroDivisionError", "division by zero"],
            trb='  File "%s", line 41, in test_dumps_task_result\n    1 / 0\n' % __file__,
        )

    def test_dumps_event(self):
        serializer = serializers.json.Serializer()

        event = Event(
            event_id="f3410fd3-660c-4e26-b433-a6c2f5bdf700",
            type="TP",
            dt=datetime.datetime(2022, 3, 12),
            data={"k": "v"},
        )
        assert (
            serializer.dumps_event(event)
            == b'{"type": "TP", "data": {"k": "v"}, "event_id": "f3410fd3-660c-4e26-b433-a6c2f5bdf700", "dt": "2022-03-12T00:00:00", "ttl": 300}'
        )

    def test_loads_event(self):
        serializer = serializers.json.Serializer()

        event = serializer.loads_event(
            b'{"type": "TP", "data": {"k": "v"}, "event_id": "f3410fd3-660c-4e26-b433-a6c2f5bdf700", "dt": "2022-03-12T00:00:00", "ttl": 300}'
        )
        assert event == Event(
            event_id="f3410fd3-660c-4e26-b433-a6c2f5bdf700",
            type="TP",
            dt=datetime.datetime(2022, 3, 12),
            data={"k": "v"},
        )
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.dt, datetime.datetime)
