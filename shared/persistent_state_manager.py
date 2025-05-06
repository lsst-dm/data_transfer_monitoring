import asyncpg
from typing import Any, List, Dict

from shared.constants import POSTGRES_CONNECTION_STRING


class PersistentStateClient:
    def __init__(self):
        self.dsn = POSTGRES_CONNECTION_STRING
        self.pool = None

    async def initialize(self):
        await self.connect()

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn)
        # await self._ensure_table()

    async def _ensure_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS kafka_messages (
                    id SERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    partition INT NOT NULL,
                    offset BIGINT NOT NULL,
                    key BYTEA,
                    value BYTEA,
                    timestamp TIMESTAMP DEFAULT NOW()
                );
            """)

    async def save_message(self, topic: str, partition: int, offset: int, key: bytes, value: bytes):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO kafka_messages (topic, partition, offset, key, value)
                VALUES ($1, $2, $3, $4, $5)
            """, topic, partition, offset, key, value)

    async def get_messages(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM kafka_messages
                WHERE topic = $1
                ORDER BY offset DESC
                LIMIT $2
            """, topic, limit)
            return [dict(row) for row in rows]

    async def delete_message_by_id(self, msg_id: int) -> int:
        """Delete a message by its unique ID. Returns number of rows deleted."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM kafka_messages WHERE id = $1
            """, msg_id)
            return int(result.split()[-1])

    async def delete_messages_by_topic(self, topic: str) -> int:
        """Delete all messages for a given topic. Returns number of rows deleted."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM kafka_messages WHERE topic = $1
            """, topic)
            return int(result.split()[-1])

    async def delete_message_by_offset(self, topic: str, partition: int, offset: int) -> int:
        """Delete a message by topic, partition, and offset. Returns number of rows deleted."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM kafka_messages
                WHERE topic = $1 AND partition = $2 AND offset = $3
            """, topic, partition, offset)
            return int(result.split()[-1])

    async def close(self):
        if self.pool:
            await self.pool.close()

