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

    def test__init_custom(self, cleanup):
        config = redis.Config(
            id="id",
            url="redis://example.com",
            timeout=123,
            verify_ssl=False,
            pool_size=50,
            push_retry_timeouts=[2],
            pull_retry_timeouts=[5],
        )
        assert config.id == "id"
        assert config.serializer.module == serializers.json
        assert config.url.get_secret_value() == "redis://example.com"
        assert config.timeout == 123
        assert config.verify_ssl is False
        assert config.pool_size == 50
        assert config.push_retry_timeouts == [2]
        assert config.pull_retry_timeouts == [5]


class TestBackend:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        backend = redis.Backend(redis.Config())
        try:
            assert isinstance(backend.serializer, serializers.json.Serializer)
            assert backend.redis_pool is not None
        finally:
            await backend.close()
