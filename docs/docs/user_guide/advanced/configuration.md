By default **Arrlio** use `local` broker/result backend/event backend.

To choose a `rabbitmq` as broker/result backend/event backend specify application config as shown below:

```python
from arrlio import Config

config = Config(
    borker="arrlio.backends.brokers.rabbitmq",
    result_backend="arrlio.backends.result_backeds.rabbitmq",
    event_backend="arrlio.backends.event_backeds.rabbitmq"
)
```

And for non default configuration options:

```python
from arrlio import Config

config = Config(
    broker={
        "module": "arrlio.backends.borkers.rabbitmq",
        "config": {
            "url": "...",
            ...
        }
    },
    ...
)
```
