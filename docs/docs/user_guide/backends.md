**Arrlio** has several builtin backends:

- arrlio.backends.local
- arrlio.backends.rabbitmq


## Local

### Settings

All environment varialbles should starts with `${ARRLIO_ENV_PREFIX}LOCAL_BACKEND_`.
!!! info 
    Default value for `ARRLIO_ENV_PREFIX` is `ARRLIO_`.

- `ID` (Config.id), default: `"arrlio"`. Backend Id.
- `SERIALIZER` (Config.serializer), default: `"arrlio.serializers.nop"`. Serializer module with `Serializer` and `Config` classes.


## RabbitMQ

### Settings

All environment varialbles should starts with `${ARRLIO_ENV_PREFIX}RMQ_BACKEND_`.

- `ID` (Config.id), default: `uuid4()`. Backend Id.
- `SERIALIZER` (Config.serializer), default: "arrlio.serializers.json". Serializer module with `Serializer` and `Config` classes.
- `URL` (Config.url), default: `"amqp://guest:guest@localhost"`. RabbitMQ server url.
- `VERIFY_SSL` (Config.verify_ssl), default: `True`.
- `TIMEOUT` (Config.timeout), default: `10`. RabbitMQ operation timeout, seconds.
- `PUSH_RETRY_TIMEOUTS` (Config.push_retry_timeouts), default: `[5, 5, 5, 5, 5]`. Retry timeout sequense for push operations, seconds.
- `PULL_RETRY_TIMEOUTS` (Config.pull_retry_timeouts), default: `itertools.repeat(5)`. Retry timeout sequense for pull operations, seconds.

- `TASKS_EXCHANGE` (Config.tasks_exchange), default: `"arrlio"`. Tasks RabbitMQ exchange.
- `TASKS_EXCHANGE_DURABLE` (Config.tasks_exchange_durable), default: `False`.
- `TASKS_QUEUE_TYPE` (Config.tasks_queue_type), default: `QueueType.CLASSIC`.
- `TASKS_QUEUE_DURABLE` (Config.tasks_queue_durable), default: `False`.
- `TASKS_QUEUE_AUTO_DELETE` (Config.tasks_queue_auto_delete), default: `True`.
- `TASKS_QUEUE_TTL` (Config.tasks_queue_ttl), default: `None`. `x-message-ttl` RabbitMQ option, seconds.
- `TASKS_PREFETCH_COUNT` (Config.tasks_prefetch_count), default: `1`. RabbitMQ prefetch count options.

- `EVENTS_EXCHANGE` (Config.events_exchange), default: `"arrlio.events"`. Events RabbitMQ exchange.
- `EVENTS_EXCHANGE_DURABLE` (Config.events_exchange_durable), default: `False`.
- `EVENTS_QUEUE_TYPE` (Config.events_queue_type), default: `QueueType.CLASSIC`.
- `EVENTS_QUEUE_DURABLE`, (Config.events_queue_durable), default: `False`.
- `EVENTS_QUEUE_AUTO_DELETE` (Config.events_queue_auto_delete), default: `False`.
- `EVENTS_QUEUE_PREFIX`, (Config.events_queue_prefix), default: `"arrlio."`.
- `EVENTS_TTL` (Config.events_ttl), default: `600`. `x-message-ttl` RabbitMQ option, seconds.
- `EVENTS_PREFETCH_COUNT` (Config.events_prefetch_count), default: `1`. RabbitMQ prefetch count options.

- `RESULTS_QUEUE_MODE` (Config.results_queue_mode), default: `ResultQueueMode.COMMON`.
- `RESULTS_QUEUE_PREFIX` (COnfig.results_queue_prefix), default: `"arrlio."`.
!!! note
    Valid only with `ResultsQueueMode.COMMON`.
- `RESULTS_QUEUE_TYPE` (Config.results_queue_type), default: `QueueType.CLASSIC`.
!!! note
    Valid only with `ResultsQueueMode.COMMON`.
- `RESULTS_QUEUE_DURABLE` (Config.results_queue_durable), default: `False`.
!!! note
    Valid only with `ResultsQueueMode.COMMON`.
- `RESULTS_TTL` (Config.results_ttl), default: `600`. Result time to live, seconds.
- `RESULTS_PREFETCH_COUNT` (Config.results_prefetch_count), default: `10`.
!!! note
    Valid only with `ResultsQueueMode.COMMON`.

