import logging

from typing import Callable, Optional, Type

from arrlio.utils import ExtendedJSONEncoder


try:
    import orjson

    _dumps = orjson.dumps

    def json_dumps(obj, cls=None):
        return _dumps(obj, default=cls)

    json_loads = orjson.loads

    JSONEncoderType = Optional[Callable]
    JSON_ENCODER = None

except ImportError:
    import json

    _dumps = json.dumps

    def json_dumps(*args, **kwds):
        return _dumps(*args, **kwds).encode()

    json_loads = json.loads

    JSONEncoderType = Optional[Type[json.JSONEncoder]]
    JSON_ENCODER = ExtendedJSONEncoder

from typing import Annotated, Any

from pydantic import Field, PlainSerializer
from pydantic_settings import SettingsConfigDict

from arrlio.serializers import base
from arrlio.settings import ENV_PREFIX


logger = logging.getLogger("arrlio.serializers.json")


Encoder = Annotated[
    JSONEncoderType,
    PlainSerializer(lambda x: f"{x}", return_type=str, when_used="json"),
]


class Config(base.Config):
    """Config for JSON serializer."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}JSON_SERIALIZER_")

    encoder: Encoder = Field(default=JSON_ENCODER)
    """Encoder class."""


class Serializer(base.Serializer):
    """JSON serializer."""

    @property
    def content_type(self) -> str | None:
        return "application/json"

    def dumps(self, data: Any, **kwds) -> bytes:
        """Dump data as json encoded string.

        Args:
            data: Data to dump.
        """

        return json_dumps(data, cls=self.config.encoder)

    def loads(self, data: bytes) -> Any:
        """Load json encoded data to Python object.

        Args:
            data: Data to load.
        """

        return json_loads(data)
