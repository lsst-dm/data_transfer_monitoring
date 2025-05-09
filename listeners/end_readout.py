from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    async def handle_message(self, message):
        msg = EndReadoutModel.from_json(message)
        expected_sensors_file = (
            await self.storage_client.contains_expected_sensors_file(
                prefix=msg.expected_sensors_folder_prefix
            )
        )
        print(
            "got end readout message. has expected sensors file: ",
            expected_sensors_file,
        )
        # print(f"End Readout: {msg.value.decode()}")
