import logging
from datetime import datetime

from prometheus_client import Histogram
from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker

log = logging.getLogger(__name__)


class FileNotificationListener(BaseKafkaListener):
    """Class for handling FileNotification event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.notification_tracker = NotificationTracker()
        self.time_of_last_message = self.get_initial_time_of_last_message()
        self.total_messages_received = Counter(
            "dtm_file_messages_received_total", "Total number of file messages received"
        )
        self.fits_files_received = Counter(
            "dtm_file_messages_received_fits_total",
            "Total number of .fits file messages received",
        )
        self.json_files_received = Counter(
            "dtm_file_messages_received_header_total",
            "Total number of .json header file messages received",
        )
        self.file_message_histogram = Histogram(
            "dtm_file_messages_received_seconds",
            "Histogram of file message receive intervals (Seconds)",
            buckets=[1, 2, 4, 8, 16],
        )

    def should_skip(self, message: FileNotificationModel):
        if not message.filepath.name.startswith("MC"):
            return True

        return False

    def get_initial_time_of_last_message(self):
        """This may be stored in postgres due to pod failover"""
        return datetime.now()

    def record_histogram(self, now: datetime):
        time_since_last_message = now - self.time_of_last_message
        self.file_message_histogram.observe(time_since_last_message.total_seconds())

    def record_metrics(self, msg: FileNotificationModel):
        now = datetime.now()

        if msg.file_type == FileNotificationModel.JSON:
            self.json_files_received.inc()

        if msg.file_type == FileNotificationModel.FITS:
            self.fits_files_received.inc()

        self.record_histogram(now)
        self.total_messages_received.inc()

        self.time_of_last_message = now

    async def handle_message(self, msg_obj, deserializer):
        msg = FileNotificationModel.from_json(msg_obj)
        log.debug(f"file notification: {msg}")
        if self.should_skip(msg):
            return
        await self.notification_tracker.add_file_notification(
            msg.storage_key, msg, msg.file_type
        )
        self.record_metrics(msg)

    async def handle_batch(self, batch, deserializer):
        log.debug("received file notification batch")
        log.debug(f"file notification batch: {batch}")
        messages = [FileNotificationModel.from_json(msg) for msg in batch]
        storage_keys = [msg.storage_key for msg in messages]
        file_types = [msg.file_type for msg in messages]
        await self.notification_tracker.add_file_notifications(storage_keys, messages, file_types)
        for msg_obj in messages:
            self.record_metrics(msg_obj)
            # await self.handle_message(msg_obj, deserializer)
