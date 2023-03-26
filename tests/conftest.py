import gc
import os
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
def cleanup():
    yield
    gc.collect()
    backends.local.Backend._Backend__shared.clear()


@pytest_asyncio.fixture(scope="function")
async def params(request, container_executor, cleanup):
    config = request.param

    address = None
    container = None

    if config["backend"]["module"] == "arrlio.backends.rabbitmq":
        f = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rabbitmq.conf")
        container = container_executor.run_wait_up(
            "rabbitmq:3-management",
            ports={"15672": "15672"},
            # volumes={f: {"bind": "/etc/rabbitmq/rabbitmq.conf"}},
        )
        address = (container.attrs["NetworkSettings"]["IPAddress"], 5672)
        config["backend"].setdefault("config", {}).update(
            {
                "url": [
                    f"amqp://guest:guest@invalid:{address[1]}",
                    f"amqp://guest:guest@{address[0]}:{address[1]}",
                ]
            }
        )
        time.sleep(0.1)
    if config["backend"]["module"] == "arrlio.backends.redis":
        container = container_executor.run_wait_up("redis:latest", command='redis-server --save "" --appendonly yes')
        address = (container.attrs["NetworkSettings"]["IPAddress"], 6379)
        config["backend"].setdefault("config", {}).update({"url": f"redis://{address[0]}:{address[1]}"})
        time.sleep(0.1)

    if address:
        try:
            utils.wait_socket_available(address, 20)
        except Exception:
            print(container.logs().decode())
            raise

    app = App(Config(**config))
    try:
        await app.init()
        yield container, app
    finally:
        await app.close()
