**Arrlio** provides three builtin backends:

- arrlio.backends.local
- arrlio.backends.rabbitmq
- arrlio.backends.redis


## Local

### Settings

All environment varialbles should starts with `{ENV_PREFIX}LOCAL_BACKEND_`.

- `NAME` (BackendConfig.name), default: "arrlio". Backend name.
- `SERIALIZER` (BackendConfig.serializer), default: "arrlio.serializers.nop.Nop". Serializer class to use.


## RabbitMQ

### Settings

All environment varialbles should starts with `{ENV_PREFIX}RMQ_BACKEND_`.

- `NAME` (BackendConfig.name), default: "arrlio". Backend name.
- `SERIALIZER` (BackendConfig.serializer), default: "arrlio.serializers.json.Json". Serializer class to use.
- `URL` (BackendConfig.url), default: "amqp://guest:guest@localhost". RabbitMQ server url.
- `TIMEOUT` (BackendConfig.timeout), default: 10. RabbitMQ operation timeout, seconds.
- `RETRY_TIMEOUTS` (BackendConfig.retry_timeouts), default: None. Retry timeouts as sequence: [1, 2, 5].
- `VERIFY_SSL` (BackendConfig.verify_ssl), default: True.
- `TASKS_EXCHANGE` (BackendConfig.tasks_exchange), default: "arrlio". Tasks RabbitMQ exchange.
- `TASKS_QUEUE_DURABLE` (BackendConfig.tasks_queue_durable), default: False.
- `TASKS_QUEUE_TTL` (BackendConfig.tasks_queue_ttl), default: None. `x-message-ttl` RabbitMQ option, seconds.
- `TASKS_PREFETCH_COUNT` (BackendConfig.tasks_prefetch_count), default: 1. RabbitMQ prefetch count options.
- `EVENTS_EXCHANGE` (BackendConfig.events_exchange), default: "arrlio". Events RabbitMQ exchange.
- `EVENTS_QUEUE_DURABLE`, (BackendConfig.events_queue_durable), default: False.
- `EVENTS_QUEUE`, (BackendConfig.events_queue), default: "arrlio.events".
- `EVENTS_QUEUE_TTL` (BackendConfig.events_queue_ttl), default: None. `x-message-ttl` RabbitMQ option, seconds.
- `EVENTS_PREFETCH_COUNT` (BackendConfig.events_prefetch_count), default: 1. RabbitMQ prefetch count options.
- `MESSAGES_PREFETCH_COUNT` (BackendConfig.messages_prefetch_count), default: 1. RabbitMQ prefetch count options.


## Redis

### Settings

- `NAME` (BackendConfig.name), default: "arrlio". Backend name.
- `SERIALIZER` (BackendConfig.serializer), default: "arrlio.serializers.json.Json". Serializer class to use.
- `URL` (BackendConfig.url), default: "redis://localhost?db=0". Redis server url.
- `TIMEOUT` (BackendConfig.timeout), default: 10. Redis operation timeout, seconds.
- `CONNECT_TIMEOUT` (BackendConfig.connect_timeout), default: 10. Redis connect timeout, seconds.
- `RETRY_TIMEOUTS` (BackendConfig.retry_timeouts), default: None. Retry timeouts as sequence: [1, 2, 5].
- `POOL_SIZE` (BackendConfig.pool_size), default: 10. Redis connection pool size.
- `VERIFY_SSL` (BackendConfig.verify_ssl), default: True.
