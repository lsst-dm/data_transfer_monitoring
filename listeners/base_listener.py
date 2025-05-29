import ssl
from abc import ABC
from abc import abstractmethod

from aiokafka import AIOKafkaConsumer

from shared import constants
from shared import config
from shared.s3_client import AsyncS3Client
from shared.log import log


class BaseKafkaListener(ABC):
    def __init__(self, topic, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, group_id=config.KAFKA_GROUP_ID, use_auth=False):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.use_auth = use_auth
        self.consumer = None
        self.storage_client = AsyncS3Client()

        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False  # TODO set to true when migrated to prompt-kafka
        self.ssl_context.verify_mode = ssl.CERT_NONE  # TODO enable when migrated to prompt-kafka

        self.auth_params = self.get_auth_params()

    def get_auth_params(self):
        if self.use_auth:
            return {
                "security_protocol": constants.SECURITY_PROTOCOL,
                "sasl_mechanism": constants.SASL_MECHANISM,
                "sasl_plain_username": constants.SASL_USERNAME,
                "sasl_plain_password": constants.SASL_PASSWORD,
                "ssl_context": self.ssl_context,
            }
        return {
            "security_protocol": "PLAINTEXT",
        }

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            **self.auth_params
        )
        log.info("starting consumer...")
        await self.consumer.start()
        log.info("consumer started successfully!")
        try:
            log.info("listening to messages...")
            async for msg in self.consumer:
                log.info(f"received message: {msg}")
                # json_string = msg.value.decode("utf-8")
                await self.handle_message(msg.value)
        finally:
            log.info("stopping consumer")
            await self.consumer.stop()

    @abstractmethod
    async def handle_message(self, msg):
        pass
