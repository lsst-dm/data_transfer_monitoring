from datetime import datetime

from prometheus_client import Histogram
from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener


class FileNotificationListener(BaseKafkaListener):
    """Class for handling FileNotification event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.time_of_last_message = self.get_initial_time_of_last_message()
        self.total_messages_recieved = Counter(
            "file_messages_recieved_total",
            "Total number of file messages recieved"
        )
        self.file_message_histogram = Histogram(
            "file_messages_recieved_seconds",
            "Histogram of file message recieve intervals (Seconds)",
            buckets=[1, 5, 10, 20, 30]
        )

    def get_initial_time_of_last_message(self):
        """This may be stored in postgres due to pod failover"""
        return datetime.now()

    def record_histogram(self, now: datetime):
        time_since_last_message = now - self.time_of_last_message
        self.file_message_histogram.observe(time_since_last_message.total_seconds())

    def record_metrics(self):
        now = datetime.now()

        self.record_histogram(now)
        self.total_messages_recieved.inc()

        self.time_of_last_message = now

    async def handle_message(self, msg):
        self.record_metrics()
        print("recieved file notification message")
        # print(f"File Notification: {msg.value.decode()}")

