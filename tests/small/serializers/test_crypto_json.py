import sys

import pytest

from arrlio import crypto, serializers
from arrlio.models import Message, Task, TaskData, TaskInstance, TaskResult


@pytest.fixture(scope="function")
def serializer():
    pri_key = crypto.generate_private_key()
    pub_key = pri_key.public_key()
    yield serializers.crypto_json.Serializer(
        encryptor=lambda x: crypto.a_encrypt(x, pub_key),
        decryptor=lambda x: crypto.a_decrypt(x, pri_key),
    )


class TestSerializer:
    def test_dumps_loads_task_instance(self, serializer):
        task_instance = Task(None, "test").instantiate(
            TaskData(task_id="2d29459b-3245-492e-977b-09043c0f1f27", queue="queue")
        )
        assert serializer.loads_task_instance(serializer.dumps_task_instance(task_instance)) == task_instance

        task_instance = Task(None, "test").instantiate(
            TaskData(task_id="2d29459b-3245-492e-977b-09043c0f1f27", queue="queue", encrypt=True)
        )
        assert serializer.loads_task_instance(serializer.dumps_task_instance(task_instance)) == task_instance

    def test_dumps_loads_task_result(self, serializer):
        task_result = TaskResult(res="ABC")
        assert serializer.loads_task_result(serializer.dumps_task_result(task_result)) == task_result

        task_result = TaskResult(res="ABC")
        assert serializer.loads_task_result(serializer.dumps_task_result(task_result, encrypt=True)) == task_result

        try:
            1 / 0
        except ZeroDivisionError:
            exc_info = sys.exc_info()
            exc = exc_info[1]
            trb = exc_info[2]

        task_result = TaskResult(exc=exc, trb=trb)
        assert serializer.loads_task_result(serializer.dumps_task_result(task_result)) == TaskResult(
            exc=["builtins", "ZeroDivisionError", "division by zero"],
            trb='  File "/vagrant/dev/arrlio/tests/small/serializers/test_crypto_json.py", line 39, in test_dumps_loads_task_result\n    1 / 0\n',
        )
        assert serializer.loads_task_result(serializer.dumps_task_result(task_result, encrypt=True)) == TaskResult(
            exc=["builtins", "ZeroDivisionError", "division by zero"],
            trb='  File "/vagrant/dev/arrlio/tests/small/serializers/test_crypto_json.py", line 39, in test_dumps_loads_task_result\n    1 / 0\n',
        )
