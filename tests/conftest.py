import collections
import gc
import time

import pytest
import pytest_asyncio

from arrlio import App, Config, backends
from tests import utils


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
    if isinstance(request.param, list):
        backend_module, config_kwds = request.param
    else:
        backend_module, config_kwds = request.param, {}

    if backend_module not in [
        backends.local,
        backends.rabbitmq,
        backends.redis,
    ]:
        raise Exception("Unsupported backend %s" % backend_module)

    address = None
    container = None

    if backend_module == backends.rabbitmq:
        container = container_executor.run_wait_up("rabbitmq:3-management", ports={"15672": "15672"})
        address = (container.attrs["NetworkSettings"]["IPAddress"], 5672)
        config_kwds.update(
            {
                "url": [
                    f"amqp://guest:guest@invalid:{address[1]}",
                    f"amqp://guest:guest@{address[0]}:{address[1]}",
                ]
            }
        )
        time.sleep(0.1)
    elif backend_module == backends.redis:
        container = container_executor.run_wait_up("redis:latest", command='redis-server --save "" --appendonly no')
        address = (container.attrs["NetworkSettings"]["IPAddress"], 6379)
        config_kwds.update({"url": f"redis://{address[0]}:{address[1]}"})
        time.sleep(0.1)

    if address:
        try:
            utils.wait_socket_available(address, 20)
        except Exception:
            print(container.logs().decode())
            raise

    BackendTuple = collections.namedtuple("BackendTuple", ["container", "module", "config_kwds"])

    try:
        yield BackendTuple(container, backend_module, config_kwds)
    finally:
        if backend_module == backends.local:
            backends.local.Backend._Backend__shared.clear()


@pytest.fixture(scope="function")
def cleanup():
    yield
    gc.collect()


@pytest_asyncio.fixture(scope="function")
async def app(backend, cleanup):
    config = Config(backend={"module": backend.module, "config": backend.config_kwds})
    app = App(config)
    try:
        yield app
    finally:
        await app.close()
