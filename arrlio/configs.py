from typing import Annotated, Optional, cast
from uuid import uuid4

from annotated_types import MinLen
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.settings import (
    BROKER,
    ENV_PREFIX,
    EVENT_BACKEND,
    EVENT_TTL,
    EXECUTOR,
    RESULT_BACKEND,
    SERIALIZER,
    TASK_ACK_LATE,
    TASK_EVENTS,
    TASK_PRIORITY,
    TASK_QUEUE,
    TASK_QUEUES,
    TASK_RESULT_RETURN,
    TASK_RESULT_TTL,
    TASK_TIMEOUT,
    TASK_TTL,
)
from arrlio.types import (
    TTL,
    BrokerModule,
    EventBackendModule,
    ExecutorModule,
    ModuleConfig,
    PluginModule,
    ResultBackendModule,
    SerializerModule,
    TaskPriority,
    Timeout,
    UniqueList,
)


class ModuleConfigValidatorMixIn:
    @field_validator("config", mode="after", check_fields=False)
    @classmethod
    def validate_config(cls, v, info):
        if "module" not in info.data:
            return v
        config_cls = info.data["module"].Config
        if isinstance(v, config_cls):
            return v
        if isinstance(v, dict):
            return config_cls(**v)
        return config_cls(**v.model_dump())


class SerializerConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """
    Config for serializer module.

    Attributes:
        module: Serializer module as module path or `SerializerModule` instance.
            [Default][arrlio.settings.SERIALIZER].
        config: Module config as `dict` or `module.Config` instances.

    Examples:
        ```python
        >>> SerializerConfig(
                module='myapp.serializers.custom_serializer',
                config={}
            )
        ```
    """

    module: SerializerModule = cast(SerializerModule, SERIALIZER)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class BrokerModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """
    Config for broker module.

    Default env prefix: `${ENV_PREFIX}BROKER_`.

    Attributes:
        module: Broker module as module path or `BrokerModule` instance.

            [Default][arrlio.settings.BROKER].

        config: Module config as `dict` or `module.Config` instances.

    Examples:
        ```python
        >>> BrokerModuleConfig(
                module='myapp.borkers.custom_broker',
                config={}
            )
        ```
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}BROKER_")

    module: BrokerModule = cast(BrokerModule, BROKER)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class ResultBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """
    Config for result backend module.

    Default env prefix: `${ENV_PREFIX}RESULT_BACKEND_`.

    Attributes:
        module: Result backend module as module path or `ResultBackendModule` instance.

            [Default][arrlio.settings.RESULT_BACKEND].

        config: Module config as `dict` or `module.Config` instances.

    Examples:
        ```python
        >>> ResultBackendModuleConfig(
                module='myapp.result_backends.custom_result_backend',
                config={}
            )
        ```
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RESULT_BACKEND_")

    module: ResultBackendModule = cast(ResultBackendModule, RESULT_BACKEND)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class EventBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """
    Config for event backend module.

    Default env prefix: `${ENV_PREFIX}EVENT_BACKEND_`.

    Attributes:
        module: Event backend module as module path or `EventBackendModule` instance.

            [Default][arrlio.settings.EVENT_BACKEND].

        config: Module config as `dict` or `module.Config` instances.

    Examples:
        ```python
        >>> EventBackendModuleConfig(
                module='myapp.event_backends.custom_event_backend',
                config={}
            )
        ```
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_BACKEND_")

    module: EventBackendModule = cast(EventBackendModule, EVENT_BACKEND)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class TaskConfig(BaseSettings):
    """
    Config for task.

    Attributes:
        queue: Default queue name for task.
            [Default][arrlio.settings.TASK_QUEUE].
        priority: Default task priority.
            [Default][arrlio.settings.TASK_PRIORITY].
        timeout: Default task excution timeout in seconds.
            [Default][arrlio.settings.TASK_TIMEOUT].
        ttl: Default task TTL in seconds.
            [Default][arrlio.settings.TASK_TTL].
        ack_late: Task ack late options.
            [Default][arrlio.settings.TASK_ACK_LATE].
        result_return: Is it necessary to return the task result by default.
            [Default][arrlio.settings.TASK_RESULT_RETURN].
        result_ttl: Result TTL in seconds.
            [Default][arrlio.settings.TASK_RESULT_TTL].
        events: Will events be generated for task. This option requires `arrlio.plugins.events` plugin.
            [Default][arrlio.settings.TASK_EVENTS].
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}TASK_")

    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: TaskPriority = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: Optional[TTL] = Field(default_factory=lambda: TASK_TTL)
    ack_late: bool = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: bool = Field(default_factory=lambda: TASK_RESULT_RETURN)
    result_ttl: Optional[TTL] = Field(default_factory=lambda: TASK_RESULT_TTL)
    events: set[str] | bool = Field(default_factory=lambda: TASK_EVENTS)


class EventConfig(BaseSettings):
    """
    Config for event.

    Default env prefix: `${ENV_PREFIX}EVENT_`.

    Attributes:
        ttl: Event TTL in seconds.
            [Default][arrlio.settings.EVENT_TTL]
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_")

    ttl: Optional[TTL] = Field(default_factory=lambda: EVENT_TTL)


class PluginModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """
    Config for plugin module.

    Attributes:
        module: Plugin module as module path or `PluginModule` instance.
        config: Module config as `dict` or `module.Config` instances.
    """

    model_config = SettingsConfigDict()

    module: PluginModule
    config: ModuleConfig = Field(default_factory=BaseSettings)


class ExecutorModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """
    Config for executor module.

    Default env prefix: `${ENV_PREFIX}EXECUTOR_`.

    Attributes:
        module: Executor module as module path or `ExecutorModule` instance.

            [Default][arrlio.settings.EXECUTOR].

        config: Module config as `dict` or `module.Config` instances.
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EXECUTOR_")

    module: ExecutorModule = cast(ExecutorModule, EXECUTOR)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class Config(BaseSettings):
    """
    Arrlio application main config.

    Attributes:
        app_id: Arrlio application Id.
        broker: Broker module config.
        result_backend: Result backend module config.
        event_backend: Event backend module config.
        task: Task config.
        event: Event config.
        task_queues: Task queues to consume.
        plugins: List of plugins.
        executor: Executor module config.
    """

    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    app_id: Annotated[str, MinLen(4)] = Field(default_factory=lambda: f"{uuid4().hex[-6:]}")
    broker: BrokerModuleConfig = Field(default_factory=BrokerModuleConfig)
    result_backend: ResultBackendModuleConfig = Field(default_factory=ResultBackendModuleConfig)
    event_backend: EventBackendModuleConfig = Field(default_factory=EventBackendModuleConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    event: EventConfig = Field(default_factory=EventConfig)
    task_queues: UniqueList[str] = Field(default_factory=lambda: TASK_QUEUES)
    plugins: list[PluginModuleConfig] = Field(default_factory=list)
    executor: ExecutorModuleConfig = Field(default_factory=ExecutorModuleConfig)
