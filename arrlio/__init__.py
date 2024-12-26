import gettext
import importlib.metadata
import logging
import os
import sys


gettext.bindtextdomain(
    "arrlio",
    localedir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "locales"),
)
gettext.textdomain("arrlio")


__version__ = importlib.metadata.version("arrlio")


logger = logging.getLogger("arrlio")

log_frmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-40s lineno:%(lineno)4d -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
logger.addHandler(log_hndl)

from arrlio import settings

# ruff: noqa: E402
from arrlio.configs import Config, TaskConfig  # noqa
from arrlio.core import App, AsyncResult, get_app, registered_tasks, task  # noqa
from arrlio.models import Event, Graph, Task, TaskInstance, TaskResult  # noqa


logger.setLevel(settings.LOG_LEVEL)
