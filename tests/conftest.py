import collections
import logging

import pytest

from arrlio import Client, ClientConfig, Executor, ExecutorConfig

from tests import utils


logger = logging.getLogger("arrlio")


@pytest.fixture(scope="function")
def container_executor():
    container_executor = utils.ContainerExecutor()
    try:
        yield container_executor
    finally:
        for container in container_executor.containers:
            container.stop()
            container.remove(v=True)


@pytest.fixture(scope="function")
def backend(request, container_executor):
    if request.param not in [
        "arrlio.backend.local",
        "arrlio.backend.rabbitmq",
        "arrlio.backend.redis",
    ]:
        raise Exception("Unsupported backend %s" % request.param)

    address = None
    container = None
    config_kwds = {}
    if request.param == "arrlio.backend.rabbitmq":
        container = container_executor.run_wait_up("rabbitmq:3-management", ports={"15672": "15672"})
        address = (container.attrs["NetworkSettings"]["IPAddress"], 5672)
        config_kwds["url"] = f"amqp://guest:guest@{address[0]}:{address[1]}"
    if request.param == "arrlio.backend.redis":
        container = container_executor.run_wait_up("redis:latest", command='redis-server --save "" --appendonly no')
        address = (container.attrs["NetworkSettings"]["IPAddress"], 6379)
        config_kwds["url"] = f"redis://{address[0]}:{address[1]}"
    if address:
        try:
            utils.wait_socket_available(address, 20)
        except Exception:
            print(container.logs().decode())
            raise

    BackendTuple = collections.namedtuple("BackendTuple", ["container", "module", "config_kwds"])
    yield BackendTuple(container, request.param, config_kwds)


@pytest.fixture(scope="function")
async def client(backend):
    config = ClientConfig(backend=backend.module)
    client = Client(config, backend_config_kwds=backend.config_kwds)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="function")
async def executor(backend):
    config = ExecutorConfig(backend=backend.module)
    executor = Executor(config, backend_config_kwds=backend.config_kwds)
    try:
        yield executor
    finally:
        await executor.stop()
