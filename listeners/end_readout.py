from listeners.base_listener import BaseKafkaListener


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""
    async def handle_message(self, msg):
        print("got end readout message")
        # print(f"End Readout: {msg.value.decode()}")

