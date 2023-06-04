**Arrlio** supports configuration from environment variables or from `Config` object.
All environment varialbles should starts with `{ENV_PREFIX}`.
Default value for `ENV_PREFIX` is `ARRLIO_`.


## General

- `APP_ID` (Config.app_id), default: `uuid4()`. Backend Id.
- `TASK_QUEUES` (Config.task_queues), default: `["arrlio.tasks"]`. List of the task queues to listen.

## Backend

- `BACKEND_MODULE` (Config.backend.module), default: `"arrlio.backends.local"`. Backend module with `Backend` and `Config` classes.
- `BACKEND_CONFIG` (Config.backend.config), default: `{}`. Config for backend.

## Task

- `TASK_QUEUE` (Config.task.queue), default: `"arrlio.tasks"`. Task queue.
- `TASK_PRIORITY` (Conig.task.priority), default: `1`. Task priority. 0 - highest priority.
- `TASK_TIMEOUT` (Config.task.timeout), default: `300`. Task execution timeout in seconds.
- `TASK_TTL` (Config.task.ttl), default: `300`. Taks time to live in seconds.
- `TASK_RESULT_RETURN` (Config.task.result_return), default: `True`. Return or not task result.
- `TASK_RESULT_TTL` (Config.task.result_ttl), default: `300`. Task result time to live, seconds.
- `TASK_EVENTS` (Config.task.events), default: `False`. Enable or disable task events.
- `TASK_EVENT_TTL` (Config.task.event_ttl), default: `300`. Task event time to live in seconds.


## Event

- `EVENT_TTL` (Config.event.ttl), default: `300`. Event time to live in seconds.


## Plugins

- `PLUGINS` (Config.plugins), default: `[]`. List of plugins.


## Executor

- `EXECUTOR_MODULE` (Config.executor.module), default: `arrlio.executor`. Executor module with `Config` and `Executor` classes.
- `EXECUTOR_CONFIG` (Config.executor.config), default: `{}`. Config for executor.


## Example

```python
from arrlio import App, Config, BackendConfig, TaskConfig

config = Config(
    backend=BackendConfig(module="arrlio.backends.local", config={"id": "Test"}),
    task_queues=["tasks.default", "tasks.backend", "tasks.frontend"],
    ...
    task=TaskConfig(
        queue="tasks.default",
        ttl=None,
        events=True,
        ...
    )
)

app = App(config)
```
