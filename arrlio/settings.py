import os


ENV_PREFIX = os.environ.get("ARRLIO_ENV_PREFIX", "ARRLIO_")
"""Environment prefix by default."""

SERIALIZER = "arrlio.serializers.json"
"""Default serializer module."""

BROKER = "arrlio.backends.brokers.local"
"""Default broker module."""
RESULT_BACKEND = "arrlio.backends.result_backends.local"
"""Default result backend module."""
EVENT_BACKEND = "arrlio.backends.event_backends.local"
"""Default evnt backend module."""

TASK_QUEUE = "arrlio.tasks"
"""Default task queue."""
TASK_MIN_PRIORITY = 1
"""Task minimum priority value."""
TASK_MAX_PRIORITY = 5
"""Task maximum priority value."""
TASK_PRIORITY = 1
"""Task default priority. Lower number means higher priority."""
TASK_TIMEOUT = 300
"""Default task timeout in seconds."""
TASK_TTL = 300
"""Default task TTL in seconds."""
TASK_ACK_LATE = False
"""Task ack late option."""
TASK_RESULT_TTL = 300
"""Default task result TTL in seconds."""
TASK_RESULT_RETURN = True
"""Is it necessary to return the task result by default."""
TASK_EVENTS = False
"""Will events be generated for the task. Requires `arrlio.plugins.events` plugin."""

EVENT_TTL = 300
"""Default event TTL in seconds."""

TASK_QUEUES = [TASK_QUEUE]
"""Default queue names to consume tasks."""

EXECUTOR = "arrlio.executor"
"""Default task executor module."""

LOG_LEVEL = "ERROR"
"""Default logger level."""
LOG_SANITIZE = True
"""Logger sanitize flag. If `True` task args and kwargs will be replaces with `<hidden>` value."""
