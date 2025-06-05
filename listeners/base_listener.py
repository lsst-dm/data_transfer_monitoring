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
    ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry = schema_registry
        self.group_id = group_id
        self.auth = auth
        self.metric_prefix = metric_prefix
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
                async for msg in self.consumer:
                    await self.handle_message(msg.value, deserializer)
        finally:
            log.info("stopping consumer")
            await self.consumer.stop()

    @abstractmethod
    async def handle_message(self, msg, deserializer=None):
        pass
