import logging

from datetime import datetime
from typing import Any
from uuid import UUID

import msgpack

from arrlio.serializers import base


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
    """Config for MessagePack serializer."""


class Serializer(base.Serializer):
    """MessagePack serializer."""

    def dumps(self, data: Any, **kwds) -> bytes:
        """Dump data as json encoded string.

        Args:
            data: Data to dump.
        """

        return msgpack_packb(data, default=encode)

    def loads(self, data: bytes) -> Any:
        """Load json encoded data to Python object.

        Args:
            data: Data to load.
        """

        return msgpack_unpackb(data, raw=False, object_hook=decode)
