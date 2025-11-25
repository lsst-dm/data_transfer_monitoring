import asyncio
from cachetools import LRUCache

from shared.notifications.stores.abstract_notification_store import (
    AbstractNotificationStore,
)


class MemoryNotificationStore(AbstractNotificationStore):
    def __init__(self, maxsize=100_000_000):
        self.cache = LRUCache(maxsize=maxsize)
        self._lock = asyncio.Lock()

    async def set(self, key, value):
        async with self._lock:
            self.cache[key] = value

    async def get(self, key):
        async with self._lock:
            return self.cache.get(key, None)

    async def pop(self, key):
        async with self._lock:
            return self.cache.pop(key, None)

    async def keys(self):
        async with self._lock:
            return list(self.cache.keys())

    async def items(self):
        async with self._lock:
            return list(self.cache.items())
