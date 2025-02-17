**Arrlio** has two builtin result backends:

- arrlio.backends.result_backends.local
- arrlio.backends.result_backends.rabbitmq


## Local

### Settings

All environment varialbles should starts with `${ARRLIO_ENV_PREFIX}LOCAL_RESULT_BACKEND_`.
!!! info 
    Default value for `ARRLIO_ENV_PREFIX` is `ARRLIO_`.

- `ID` (Config.id), default: autogenerated. Result backend Id.


## RabbitMQ

### Settings

All environment varialbles should starts with `${ARRLIO_ENV_PREFIX}RABBITMQ_RESULT_BACKEND_`.

- `ID` (Config.id), default: autogenerated. Backend Id.
- `SERIALIZER` (Config.serializer), default: "arrlio.serializers.json". Serializer module with `Serializer` and `Config` classes.
- `URL` (Config.url), default: `"amqp://guest:guest@localhost"`. RabbitMQ server url.
- `TIMEOUT` (Config.timeout), default: `10`. RabbitMQ operation timeout, seconds.
- `PUSH_RETRY_TIMEOUTS` (Config.push_retry_timeouts), default: `[5, 5, 5, 5, 5]`. Retry timeout sequense for push operations, seconds.
- `PULL_RETRY_TIMEOUTS` (Config.pull_retry_timeouts), default: `itertools.repeat(5)`. Retry timeout sequense for pull operations, seconds.
- `REPLY_TO_MODE` (Config.reply_to_mode), default: `ReplyToMode.COMMON_QUEUE`.
- `EXCHANGE` (Config.exchange), default: `"arrlio"`. RabbitMQ exchange for results.
- `EXCHANGE_DURABLE` (Config.exchange_durable), default: `False`.
- `QUEUE_PREFIX` (Config.queue_prefix), default: `"arrlio."`.
!!! note
    Valid only with `ReplyToMode.COMMON_QUEUE`.
- `QUEUE_TYPE` (Config.queue_type), default: `QueueType.CLASSIC`.
!!! note
    Valid only with `ReplyToMode.COMMON_QUEUE`.
- `QUEUE_DURABLE` (Config.queue_durable), default: `False`.
!!! note
    Valid only with `ReplyToMode.COMMON_QUEUE`.
- `PREFETCH_COUNT` (Config.prefetch_count), default: `10`.
!!! note
    Valid only with `ReplyToMode.COMMON_QUEUE`.
- `RESULT_TTL` (Config.result_ttl), default: `600`. Result time to live, seconds.
