import asyncio
import gc
import logging
import platform

import httpx
import pytest
import pytest_asyncio

from arrlio import App, Config, logger, settings
from tests import utils


# import os


logger.setLevel(logging.DEBUG)
settings.LOG_SANITIZE = False


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


@pytest_asyncio.fixture(scope="function")
async def params(request, container_executor, cleanup):
    config = request.param

    address = None
    container = None

    if (
        "rabbitmq" in config.get("broker", {}).get("module", "")
        or "rabbitmq" in config.get("result_backend", {}).get("module", "")
        or "rabbitmq" in config.get("event_backend", {}).get("module", "")
    ):
        # f = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rabbitmq.conf")
        container = container_executor.run_wait_up(
            "rabbitmq:3-management",
            ports={"5672": "5672", "15672": "15672"},
            # volumes={f: {"bind": "/etc/rabbitmq/rabbitmq.conf"}},
        )
        if platform.system() == "Darwin":
            address = "127.0.0.1", 5672
        else:
            address = container.attrs["NetworkSettings"]["IPAddress"], 5672
        for key in ("broker", "result_backend", "event_backend"):
            if "rabbitmq" not in config.get(key, {}).get("module", ""):
                continue
            config[key].setdefault("config", {})
            config[key]["config"].update(
                {
                    "url": [
                        # f"amqp://guest:guest@invalid:{address[1]}",
                        f"amqp://guest:guest@{address[0]}:{address[1]}",
                    ]
                }
            )
        api = httpx.Client(base_url=f"http://{address[0]}:15672", auth=("guest", "guest"))
        for _ in range(20):
            try:
                resp = api.get(f"/api/vhosts")
                if resp.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
        else:
            raise Exception

    if address:
        try:
            utils.wait_socket_available(address, 20)
        except Exception:
            print("\n")
            print(container.logs().decode())
            raise

    app = App(Config(**config))
    try:
        await app.init()
        yield container, app
    finally:
        await app.close()
