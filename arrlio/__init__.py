import logging
import sys


logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

log_frmt = logging.Formatter("%(asctime)s %(levelname)s [arrlio] %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
logger.addHandler(log_hndl)


__version__ = "0.11.0"

__tasks__ = {}


from arrlio.core import App, AsyncResult, logger, task  # noqa
from arrlio.exc import NotFoundError, TaskError, TaskNoResultError, TaskTimeoutError  # noqa
from arrlio.models import Graph, TaskResult  # noqa
from arrlio.settings import Config, MessageConfig, TaskConfig  # noqa
