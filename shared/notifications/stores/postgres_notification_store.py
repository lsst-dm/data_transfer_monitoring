from shared.notifications.stores.abstract_notification_store import (
    AbstractNotificationStore,
)


class PostgresNotificationStore(AbstractNotificationStore):
    def __init__(self, table_name):
        self.table_name = table_name
        # Initialize DB connection here

    async def set(self, key, value):
        # Implement upsert logic
        pass

    async def get(self, key):
        # Implement SELECT logic
        pass

    async def pop(self, key):
        # Implement SELECT+DELETE logic
        pass

    async def keys(self):
        # Implement SELECT keys logic
        pass

    async def items(self):
        # Implement SELECT * logic
        pass
