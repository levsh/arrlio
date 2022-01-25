import collections
import logging

import pytest

from arrlio import App, Config

from tests import utils


logger = logging.getLogger("arrlio")


@pytest.fixture(scope="function")
def container_executor():
    _container_executor = utils.ContainerExecutor()
    try:
        yield _container_executor
    finally:
        for container in _container_executor.containers:
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
async def app(backend):
    config = Config(backend=backend.module)
    app = App(config, backend_config_kwds=backend.config_kwds)
    try:
        yield app
    finally:
        await app.close()
        import asyncio
        await asyncio.sleep(1)
