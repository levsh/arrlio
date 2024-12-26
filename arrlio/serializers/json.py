import logging

from importlib.util import find_spec
from typing import Annotated, Any, Callable, Optional, Type

from pydantic import Field, PlainSerializer
from pydantic_settings import SettingsConfigDict

from arrlio.serializers import base
from arrlio.settings import ENV_PREFIX
from arrlio.utils import JSONEncoder, json_dumps_bytes, json_loads


logger = logging.getLogger("arrlio.serializers.json")


if find_spec("orjson") is not None:
    JSONEncoderType = Optional[Callable]
else:
    import json

    JSONEncoderType = Optional[Type[json.JSONEncoder]]


Encoder = Annotated[
    JSONEncoderType,
    PlainSerializer(lambda x: f"{x}", return_type=str, when_used="json"),
]


class Config(base.Config):
    """Config for JSON serializer."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}JSON_SERIALIZER_")

    encoder: Encoder = Field(default=JSONEncoder)
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

        return json_dumps_bytes(data, encoder=self.config.encoder)

    def loads(self, data: bytes) -> Any:
        """Load json encoded data to Python object.

        Args:
            data: Data to load.
        """

        return json_loads(data)
