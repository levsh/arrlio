# import asyncio
# from unittest import mock

import pytest

from arrlio import serializers
from arrlio.backends import redis


class TestConfig:
    def test__init(self, cleanup):
        config = redis.Config()
        assert config.id
        assert config.serializer.module == serializers.json
        assert config.url.get_secret_value() == redis.URL
        assert config.timeout == redis.TIMEOUT
        assert config.verify_ssl is True
        assert config.pool_size == redis.POOL_SIZE
        assert config.push_retry_timeouts
        assert config.pull_retry_timeouts


class TestBackend:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        backend = redis.Backend(redis.Config())
        try:
            assert isinstance(backend.serializer, serializers.json.Serializer)
            assert backend.redis_pool is not None
        finally:
            await backend.close()
