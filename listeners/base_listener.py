import logging
import ssl
from abc import ABC
from abc import abstractmethod

import httpx
from aiokafka import AIOKafkaConsumer
from kafkit.registry import Deserializer
from kafkit.registry.httpx import RegistryApi

from shared.s3_client import AsyncS3Client

log = logging.getLogger(__name__)


class BaseKafkaListener(ABC):
    def __init__(
        self,
        topic=None,
        bootstrap_servers=None,
        schema_registry=None,
        group_id=None,
        auth=None,
        metric_prefix="dtm",
        batch_size=700,
        batch_timeout_ms=1000,
        enable_batch_processing=False,
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry = schema_registry
        self.group_id = group_id
        self.auth = auth
        self.metric_prefix = metric_prefix
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.enable_batch_processing = enable_batch_processing
        self.consumer = None
        self.storage_client = AsyncS3Client()

        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = (
            False  # TODO set to true when migrated to prompt-kafka
        )
        self.ssl_context.verify_mode = (
            ssl.CERT_NONE
        )  # TODO enable when migrated to prompt-kafka

        self.auth_params = self.get_auth_params()

    def get_auth_params(self):
        if self.auth:
            return {**self.auth, "ssl_context": self.ssl_context}
        return {
            "security_protocol": "PLAINTEXT",
        }

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            max_poll_records=self.batch_size if self.enable_batch_processing else 500,
            **self.auth_params,
        )
        log.info("starting consumer...")
        await self.consumer.start()
        log.info("consumer started successfully!")
        try:
            async with httpx.AsyncClient() as client:
                if self.schema_registry:
                    registry_api = RegistryApi(http_client=client, url=self.schema_registry)
                    deserializer = Deserializer(registry=registry_api)
                else:
                    deserializer = None
                log.info("listening to messages...")
                if self.enable_batch_processing:
                    while True:
                        message_batch = await self.consumer.getmany(
                            timeout_ms=self.batch_timeout_ms,
                            max_records=self.batch_size
                        )
                        if message_batch:
                            # Convert message batch to list of message values
                            batch_messages = []
                            for topic_partition, messages in message_batch.items():
                                batch_messages.extend([msg.value for msg in messages])

                            if batch_messages:
                                await self.handle_batch(batch_messages, deserializer)
                else:
                    async for msg in self.consumer:
                        await self.handle_message(msg.value, deserializer)
        finally:
            log.info("stopping consumer")
            await self.consumer.stop()

    @abstractmethod
    async def handle_message(self, msg, deserializer=None):
        """Handle a single message. Required when enable_batch_processing=False."""
        pass

    async def handle_batch(self, messages, deserializer=None):
        """Handle a batch of messages. Override this when enable_batch_processing=True."""
        # Default implementation: process each message individually
        for message in messages:
            await self.handle_message(message, deserializer)
