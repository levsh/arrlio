import os

from types import ModuleType
from unittest import mock

import pytest

from pydantic import ValidationError

from arrlio import configs


def test_broker_config():
    env = {
        "ARRLIO_LOCAL_BROKER_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.BrokerModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.brokers.local"
        assert isinstance(config.config, config.module.Config)
        assert config.config.id
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert config.model_dump_json() == (
            '{"module":"arrlio.backends.brokers.local","config":{"id":"arrlio","pool_size":100}}'
        )

    env = {
        "ARRLIO_LOCAL_BROKER_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.BrokerModuleConfig(module="arrlio.backends.brokers.rabbitmq")
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.brokers.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.push_retry_timeouts == [5, 5, 5, 5]
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert isinstance(config.model_dump()["config"]["serializer"]["module"], ModuleType)
        assert config.model_dump_json()
        with pytest.raises(ValidationError):
            config = configs.BrokerModuleConfig(module="arrlio.backends.brokers.invalid")
        with pytest.raises(
            ValidationError,
            match=r".*Value error, module doesn't provide required attribute 'Broker'.*",
        ):
            config = configs.BrokerModuleConfig(module="sys")

    env = {
        "ARRLIO_BROKER_MODULE": "arrlio.backends.brokers.rabbitmq",
        "ARRLIO_RABBITMQ_BROKER_SERIALIZER_MODULE": "arrlio.serializers.msgpack",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.BrokerModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.brokers.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.serializer.module.__name__ == "arrlio.serializers.msgpack"

    config = configs.BrokerModuleConfig(config={})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.brokers.local"
    assert isinstance(config.config, config.module.Config)

    config = configs.BrokerModuleConfig(config={"id": "Test"})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.brokers.local"
    assert isinstance(config.config, config.module.Config)
    assert config.config.id == "Test"


def test_result_backend_config():
    env = {
        "ARRLIO_LOCAL_RESULT_BACKEND_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.ResultBackendModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.result_backends.local"
        assert isinstance(config.config, config.module.Config)
        assert config.config.id
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert config.model_dump_json() == (
            '{"module":"arrlio.backends.result_backends.local","config":{"id":"arrlio"}}'
        )

    env = {
        "ARRLIO_LOCAL_RESULT_BACKEND_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.ResultBackendModuleConfig(module="arrlio.backends.result_backends.rabbitmq")
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.result_backends.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.push_retry_timeouts == [5, 5, 5, 5]
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert isinstance(config.model_dump()["config"]["serializer"]["module"], ModuleType)
        assert config.model_dump_json()
        with pytest.raises(ValidationError):
            config = configs.ResultBackendModuleConfig(module="arrlio.backends.result_backends.invalid")
        with pytest.raises(
            ValidationError,
            match=r".*Value error, module doesn't provide required attribute 'ResultBackend'.*",
        ):
            config = configs.ResultBackendModuleConfig(module="sys")

    env = {
        "ARRLIO_RESULT_BACKEND_MODULE": "arrlio.backends.result_backends.rabbitmq",
        "ARRLIO_RABBITMQ_RESULT_BACKEND_SERIALIZER_MODULE": "arrlio.serializers.msgpack",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.ResultBackendModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.result_backends.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.serializer.module.__name__ == "arrlio.serializers.msgpack"

    config = configs.ResultBackendModuleConfig(config={})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.result_backends.local"
    assert isinstance(config.config, config.module.Config)

    config = configs.ResultBackendModuleConfig(config={"id": "Test"})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.result_backends.local"
    assert isinstance(config.config, config.module.Config)
    assert config.config.id == "Test"


def test_event_backend_config():
    env = {
        "ARRLIO_LOCAL_EVENT_BACKEND_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.EventBackendModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.event_backends.local"
        assert isinstance(config.config, config.module.Config)
        assert config.config.id
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert config.model_dump_json() == (
            '{"module":"arrlio.backends.event_backends.local","config":{"id":"arrlio"}}'
        )

    env = {
        "ARRLIO_LOCAL_EVENT_BACKEND_ID": "arrlio",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.EventBackendModuleConfig(module="arrlio.backends.event_backends.rabbitmq")
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.event_backends.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.push_retry_timeouts == [5, 5, 5, 5]
        assert isinstance(config.model_dump()["module"], ModuleType)
        assert isinstance(config.model_dump()["config"]["serializer"]["module"], ModuleType)
        assert config.model_dump_json()
        with pytest.raises(ValidationError):
            config = configs.EventBackendModuleConfig(module="arrlio.backends.event_backends.invalid")
        with pytest.raises(
            ValidationError,
            match=r".*Value error, module doesn't provide required attribute 'EventBackend'.*",
        ):
            config = configs.EventBackendModuleConfig(module="sys")

    env = {
        "ARRLIO_EVENT_BACKEND_MODULE": "arrlio.backends.event_backends.rabbitmq",
        "ARRLIO_RABBITMQ_EVENT_BACKEND_SERIALIZER_MODULE": "arrlio.serializers.msgpack",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.EventBackendModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.event_backends.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.serializer.module.__name__ == "arrlio.serializers.msgpack"

    config = configs.EventBackendModuleConfig(config={})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.event_backends.local"
    assert isinstance(config.config, config.module.Config)

    config = configs.EventBackendModuleConfig(config={"id": "Test"})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.event_backends.local"
    assert isinstance(config.config, config.module.Config)
    assert config.config.id == "Test"


def test_executor_config():
    config = configs.ExecutorModuleConfig()
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.executor"
    assert isinstance(config.config, config.module.Config)

    config = configs.ExecutorModuleConfig(module="arrlio.executor")
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.executor"
    assert isinstance(config.config, config.module.Config)

    with pytest.raises(ValidationError):
        config = configs.ExecutorModuleConfig(module="invalid")

    with pytest.raises(
        ValidationError,
        match=r".*Value error, module doesn't provide required attribute 'Executor'.*",
    ):
        config = configs.ExecutorModuleConfig(module="sys")

    env = {"ARRLIO_EXECUTOR_MODULE": "arrlio.executor"}
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.ExecutorModuleConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.executor"
        assert isinstance(config.config, config.module.Config)
