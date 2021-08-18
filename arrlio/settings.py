from typing import List

from pydantic import BaseSettings, Field

from arrlio.tp import BackendT, PositiveIntT, PriorityT, TimeoutT


BACKEND = "arrlio.backend.local"

TASK_BIND = False
TASK_QUEUE = "arrlio.tasks"
TASK_PRIORITY = 1
TASK_TIMEOUT = 300
TASK_TTL = 300
TASK_ACK_LATE = False

RESULT_TTL = 300
RESULT_RETURN = True
RESULT_ENCRYPT = None

WORKER_TASK_QUEUES = ["arrlio.tasks"]
WORKER_MESSAGE_QUEUES = ["arrlio.messages"]
WORKER_POOL_SIZE = 100


class TaskConfig(BaseSettings):
    bind: bool = Field(default_factory=lambda: TASK_BIND)
    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: PriorityT = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: TimeoutT = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: PositiveIntT = Field(default_factory=lambda: TASK_TTL)
    ack_late: bool = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: bool = Field(default_factory=lambda: RESULT_RETURN)
    result_encrypt: bool = Field(default_factory=lambda: RESULT_ENCRYPT)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_TASK_"


class ResultConfig(BaseSettings):
    ttl: PositiveIntT = Field(default_factory=lambda: RESULT_TTL)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_RESULT_"


class ClientConfig(BaseSettings):
    backend: BackendT = Field(default_factory=lambda: BACKEND, env="ARRLIO_BACKEND")
    task: TaskConfig = Field(default_factory=TaskConfig)
    result: ResultConfig = Field(default_factory=ResultConfig)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_CLIENT_"


class ExecutorConfig(BaseSettings):
    backend: BackendT = Field(default_factory=lambda: BACKEND, env="ARRLIO_BACKEND")
    task_queues: List[str] = Field(default_factory=lambda: WORKER_TASK_QUEUES)
    message_queues: List[str] = Field(default_factory=lambda: WORKER_MESSAGE_QUEUES)
    pool_size: PositiveIntT = Field(default_factory=lambda: WORKER_POOL_SIZE)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_EXECUTOR_"
