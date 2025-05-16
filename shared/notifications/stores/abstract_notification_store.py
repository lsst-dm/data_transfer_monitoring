from abc import ABC, abstractmethod


class AbstractNotificationStore(ABC):
    @abstractmethod
    async def set(self, key, value):
        pass

    @abstractmethod
    async def get(self, key):
        pass

    @abstractmethod
    async def pop(self, key):
        pass

    @abstractmethod
    async def keys(self):
        pass

    @abstractmethod
    async def items(self):
        pass
