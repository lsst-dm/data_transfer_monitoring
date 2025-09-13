import logging
import json
import asyncio
import time
from datetime import datetime
from datetime import timezone

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker
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
        self.notification_tracker = NotificationTracker()
        self.task_processor = TaskProcessor.get_instance()

        # Only initialize Kafka metrics if enabled
        if self.should_run_kafka_metrics:
            self.kafka_metrics_processor = KafkaMetrics()
        else:
            self.kafka_metrics_processor = None

        self.s3_metrics = S3Metrics()

    async def process_end_readout(self, msg):
        path_prefix = msg.expected_sensors_folder_prefix
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            log.info(f"Did not find expected sensors file for path prefix: {path_prefix}")
            return
        expected_fits_files, expected_json_files = sensors.get_expected_file_keys()
        total_expected_sensors = len(expected_fits_files) + len(expected_json_files)

        # Only run Kafka metrics if enabled
        if self.should_run_kafka_metrics:
            self.kafka_metrics_processor.increment_expected_files(total_expected_sensors)

        resolved = await self.notification_tracker.handle_end_readout(
            msg.id, expected_fits_files, expected_json_files, msg, sensors
        )

        return resolved

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

        resolved = await self.process_end_readout(msg)
        resolved_pending_end_readouts = (
            await self.notification_tracker.resolve_pending_end_readouts()
        )
        resolved_orphaned_end_readouts = (
            await self.notification_tracker.try_resolve_orphaned_end_readouts()
        )
        total_resolved_end_readouts = resolved_pending_end_readouts + resolved_orphaned_end_readouts
        if resolved:
            total_resolved_end_readouts.append(resolved)

        # Only run Kafka metrics if enabled
        if self.should_run_kafka_metrics:
            self.kafka_metrics_processor.increment_end_readouts_resolved(len(total_resolved_end_readouts), day_obs)
            for readout in total_resolved_end_readouts:
                self.kafka_metrics_processor.record_metrics_for_resolved_end_readout(readout)

            orphan_data = await self.notification_tracker.get_orphans_data()
            await self.kafka_metrics_processor.record_metrics_for_orphans(orphan_data)
