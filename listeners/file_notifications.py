from datetime import datetime

from prometheus_client import Histogram
from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener
from models.file_notification import FileNotificationModel


class FileNotificationListener(BaseKafkaListener):
    """Class for handling FileNotification event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.time_of_last_message = self.get_initial_time_of_last_message()
        self.total_messages_recieved = Counter(
            "file_messages_recieved_total",
            "Total number of file messages recieved"
        )
        self.fits_files_recieved = Counter(
            "file_messages_recieved_fits_total",
            "Total number of .fits file messages recieved"
        )
        self.json_files_recieved = Counter(
            "file_messages_recieved_header_total",
            "Total number of .json header file messages recieved"
        )
        self.file_message_histogram = Histogram(
            "file_messages_recieved_seconds",
            "Histogram of file message recieve intervals (Seconds)",
            buckets=[1, 5, 10, 20, 30]
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
            self.json_files_recieved.inc()

        if msg.file_type == FileNotificationModel.FITS:
            self.fits_files_recieved.inc()

        self.record_histogram(now)
        self.total_messages_recieved.inc()

        self.time_of_last_message = now

    async def handle_message(self, msg_obj):
        msg = FileNotificationModel.from_json(msg_obj)
        if self.should_skip(msg):
            return
        self.record_metrics(msg)
        print("recieved file notification message")
        # print(f"File Notification: {msg.value.decode()}")

