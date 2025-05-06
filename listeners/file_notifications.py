from listeners.base_listener import BaseKafkaListener


class FileNotificationListener(BaseKafkaListener):
    """Class for handling FileNotification event"""
    async def handle_message(self, msg):
        print("recieved file notification message")
        # print(f"File Notification: {msg.value.decode()}")

