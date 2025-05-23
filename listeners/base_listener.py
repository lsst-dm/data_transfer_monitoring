import ssl
from abc import ABC
from abc import abstractmethod
import json

from aiokafka import AIOKafkaConsumer

from shared import constants
from shared import config
from shared.persistent_state_manager import PersistentStateClient
from shared.aws_client import AsyncS3Client


class BaseKafkaListener(ABC):
    def __init__(self, topic, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, group_id=config.KAFKA_GROUP_ID):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = None
        self.state_manager = PersistentStateClient()
        self.storage_client = AsyncS3Client(region_name="us-west-2")

        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False  # TODO set to true when migrated to prompt-kafka
        self.ssl_context.verify_mode = ssl.CERT_NONE  # TODO enable when migrated to prompt-kafka

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            security_protocol="PLAINTEXT",
            # security_protocol=constants.SECURITY_PROTOCOL,
            # sasl_mechanism=constants.SASL_MECHANISM,
            # sasl_plain_username=constants.SASL_USERNAME,
            # sasl_plain_password=constants.SASL_PASSWORD,
            # ssl_context=self.ssl_context,
        )
        await self.initialize()
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                json_string = msg.value.decode("utf-8")
                json_message = json.loads(json_string)
                await self.handle_message(json_string)
        finally:
            await self.consumer.stop()

    async def initialize(self):
        await self.state_manager.initialize()
        await self.storage_client.initialize()

    @abstractmethod
    async def handle_message(self, msg):
        pass
