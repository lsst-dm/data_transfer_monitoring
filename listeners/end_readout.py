import logging
import json
import asyncio
import time
from datetime import datetime
from datetime import timezone

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.file_notification import FileNotificationModel
from shared.task_processor import TaskProcessor
from shared.metrics.kafka_metrics import KafkaMetrics
from shared.metrics.s3_metrics import S3Metrics
from shared.utils.day_of_observation import get_observation_day
from shared import constants

log = logging.getLogger(__name__)


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, should_run_kafka_metrics=False, **kwargs):
        super().__init__(*args, **kwargs)

        self.should_run_kafka_metrics = should_run_kafka_metrics
        self.task_processor = TaskProcessor.get_instance()

        # Only initialize Kafka metrics if enabled
        if self.should_run_kafka_metrics:
            self.kafka_metrics_processor = KafkaMetrics()
        else:
            self.kafka_metrics_processor = None

        self.s3_metrics = S3Metrics()

    def should_skip(self, msg):
        should_skip = not msg.image_source == "MC"
        if should_skip:
            log.info(f"skipping end readout, image source is: {msg.image_source}")
            return True
        return False


    async def handle_message(self, message, deserializer):
        if deserializer:
            message = await deserializer.deserialize(data=message)
            message = message["message"]
        else:
            message = json.loads(message)
        msg = EndReadoutModel.from_dict(message)
        log.info(f"end readout message: {msg}")
        # if self.should_skip(msg):
        #     return
        await self.task_processor.add_task(
            self.s3_metrics.determine_missing_files_in_s3(msg)
        )
        await self.task_processor.add_task(
            self.s3_metrics.record_metrics_from_s3(msg)
        )
        day_obs = get_observation_day()

        # Only run Kafka metrics if enabled
        if self.should_run_kafka_metrics:
            self.kafka_metrics_processor.increment_end_readouts_received(day_obs)
