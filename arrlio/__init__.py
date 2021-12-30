import logging
import sys


log_frmt = logging.Formatter("%(asctime)s [arrlio] %(levelname)s %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
logger = logging.getLogger("arrlio")
logger.addHandler(log_hndl)
logger.setLevel("INFO")


__version__ = "0.7.2"

__tasks__ = {}


from arrlio.core import MessageConsumer, MessageProducer, TaskConsumer, TaskProducer, logger, task  # noqa
from arrlio.exc import NotFoundError, TaskError, TaskNoResultError, TaskTimeoutError  # noqa
from arrlio.settings import MessageConsumerConfig, MessageProducerConfig, TaskConsumerConfig, TaskProducerConfig  # noqa
