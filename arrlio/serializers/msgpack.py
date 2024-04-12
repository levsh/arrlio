import logging
from datetime import datetime
from typing import Any
from uuid import UUID

import msgpack

from arrlio.serializers import base
from arrlio.serializers.json import Serializer

logger = logging.getLogger("arrlio.serializers.msgpack")

msgpack_packb = msgpack.packb
msgpack_unpackb = msgpack.unpackb


def encode(obj):
    if isinstance(obj, datetime):
        return {"__datetime__": True, "as_str": obj.isoformat()}
    if isinstance(obj, UUID):
        return {"__uuid__": True, "as_str": f"{obj}"}
    return obj


def decode(obj):
    if "__datetime__" in obj:
        obj = datetime.fromisoformat(obj["as_str"])
    if "__uuid__" in obj:
        return obj["as_str"]
    return obj


class Config(base.Config):
    pass


class Serializer(Serializer):  # pylint: disable=function-redefined
    """Msgpack serializer class."""

    def dumps(self, data: Any, **kwds) -> bytes:
        """Dumps data as json encoded string.

        Args:
            data: Data to dumps.
        """

        return msgpack_packb(data, default=encode)

    def loads(self, data: bytes) -> Any:
        """Loads json encoded data to Python object.

        Args:
            data: Data to loads.
        """

        return msgpack_unpackb(data, raw=False, object_hook=decode)
