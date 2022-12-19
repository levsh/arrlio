import logging
import sys

logger = logging.getLogger("arrlio")
logger.setLevel("WARNING")

log_frmt = logging.Formatter("%(asctime)s %(levelname)8s %(name)30s -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
logger.addHandler(log_hndl)


__version__ = "0.15.1"

# pylint: disable=wrong-import-position
from arrlio.core import App, AsyncResult, registered_tasks, task  # noqa
from arrlio.exc import NotFoundError, TaskError, TaskNoResultError, TaskTimeoutError  # noqa
from arrlio.models import Graph, TaskResult  # noqa
from arrlio.settings import Config, MessageConfig, TaskConfig  # noqa
