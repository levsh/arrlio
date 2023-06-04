By default **Arrlio** use `local` backend. It is not useful for real usage.

To choose a `rabbitmq` as backend specify application config as show below:

```python
from arrlio import Config

config = Config(module="arrlio.backends.rabbitmq")
```

And for non default Backend configuration options:

```python
from arrlio import Config

config = Config(
    module="arrlio.backends.rabbitmq",
    config={
        "url": "...",
        "verify_ssl": ...,
        ...
    },
)
```
