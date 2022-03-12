**Arrlio** supports configuration from environment variables or from `Config` object.
All environment varialbles should starts with `{ENV_PREFIX}`.
Default value for `ENV_PREFIX` is `ARRLIO_`.


## General

- `BACKEND` (Config.backend), default: "arrlio.backends.local". Backend module.
- `TASK_QUEUES` (Config.task_queues), default: ["arrlio.tasks"]. Task queues to listen.
- `MESSAGE_QUEUES` (Config.message_queues), defaut: ["arrlio.messages"]. Message queues to listen.
- `POOL_SIZE` (Config.pool_size), default: 100. Worker pool size.


## Task

- `TASK_QUEUE` (Config.task.queue), default: "arrlio.tasks". Task queue.
- `TASK_PRIORITY` (Conig.task.priority), default: 1. Task priority. 0 - highest priority.
- `TASK_TIMEOUT` (Config.task.timeout), default: 300. Task execution timeout in seconds.
- `TASK_TTL` (Config.task.ttl), default: 300. Taks time to live in seconds.
- `TASK_RESULT_RETURN` (Config.task.result_return), default: True. Return or not task result.
- `TASK_RESULT_TTL` (Config.task.result_ttl), default: 300. Task result time to live, seconds.
- `EVENTS` (Config.task.events), default: False. Enable or disable task events.
- `EVENT_TTL` (Config.task.event_ttl), default: 300. Task event time to live in seconds.


## Message

- `MESSAGE_EXCHANGE` (Config.message.exchange), default: "arrlio.messages". Message exchange.
- `MESSAGE_PRIORITY` (Config.message.priority), default: 1. Message priority. 0 - highest priority.
- `MESSAGE_TTL` (Config.message.ttl), default: 300. Message time to live in seconds.


## Example

```python
from arrlio import App, Config, MessageConfig, TaskConfig

config = Config(
    backend="arrlio.backends.local",
    task_queues=["tasks.default", "tasks.backend", "tasks.frontend"],
    ...
    task=TaskConfig(
        queue="tasks.default",
        ttl=None,
        events=True,
        ...
    )
    message=MessageConfig(
        queue="messages.default",
        ...
    )
)

app = App(config)
```
