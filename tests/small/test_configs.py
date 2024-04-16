import os
from types import ModuleType
from unittest import mock

import pytest
from pydantic import ValidationError

from arrlio import configs


def test_backend_config():
    config = configs.BackendConfig()
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.local"
    assert isinstance(config.config, config.module.Config)
    assert config.config.id == "arrlio"
    assert isinstance(config.model_dump()["module"], ModuleType)
    assert (
        config.model_dump_json()
        == '{"module":"arrlio.backends.local","config":{"id":"arrlio","serializer":{"module":"arrlio.serializers.nop","config":{}},"pool_size":100}}'
    )

    config = configs.BackendConfig(module="arrlio.backends.rabbitmq")
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.rabbitmq"
    assert isinstance(config.config, config.module.Config)
    assert config.config.push_retry_timeouts == [5, 5, 5, 5]
    assert isinstance(config.model_dump()["module"], ModuleType)
    assert isinstance(config.model_dump()["config"]["serializer"]["module"], ModuleType)
    assert config.model_dump_json()

    with pytest.raises(ValidationError):
        config = configs.BackendConfig(module="arrlio.backends.invalid")

    with pytest.raises(ValidationError, match=r".*Value error, module doesn't provide required attribute 'Backend'.*"):
        config = configs.BackendConfig(module="sys")

    env = {
        "ARRLIO_BACKEND_MODULE": "arrlio.backends.rabbitmq",
        "ARRLIO_SERIALIZER_MODULE": "arrlio.serializers.msgpack",
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.BackendConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.backends.rabbitmq"
        assert isinstance(config.config, config.module.Config)
        assert config.config.serializer.module.__name__ == "arrlio.serializers.msgpack"

    config = configs.BackendConfig(config={})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.local"
    assert isinstance(config.config, config.module.Config)

    config = configs.BackendConfig(config={"id": "Test"})
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.backends.local"
    assert isinstance(config.config, config.module.Config)
    assert config.config.id == "Test"


def test_executor_config():
    config = configs.ExecutorConfig()
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.executor"
    assert isinstance(config.config, config.module.Config)

    config = configs.ExecutorConfig(module="arrlio.executor")
    assert isinstance(config.module, ModuleType)
    assert config.module.__name__ == "arrlio.executor"
    assert isinstance(config.config, config.module.Config)

    with pytest.raises(ValidationError):
        config = configs.ExecutorConfig(module="invalid")

    with pytest.raises(ValidationError, match=r".*Value error, module doesn't provide required attribute 'Executor'.*"):
        config = configs.ExecutorConfig(module="sys")

    env = {"ARRLIO_EXECUTOR_MODULE": "arrlio.executor"}
    with mock.patch.dict(os.environ, env, clear=True):
        config = configs.ExecutorConfig()
        assert isinstance(config.module, ModuleType)
        assert config.module.__name__ == "arrlio.executor"
        assert isinstance(config.config, config.module.Config)
