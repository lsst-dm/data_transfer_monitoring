from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    async def get_ufos(self, path_prefix: str):
        expected_sensors_data = (
            await self.storage_client.download_and_parse_expected_sensors_file(
                prefix=path_prefix
            )
        )
        print("expected sensors: ", expected_sensors_data)
        expected_sensors = expected_sensors_data.expected_sensors

        # construct filepaths for each sensor and see if it exists

        return expected_sensors

    async def handle_message(self, message):
        msg = EndReadoutModel.from_json(message)

        # parse expected sensors file and make sure all of the files are in the folder
        # if any are missing, then increment counter
        print(
            "got end readout message. has expected sensors file: "
        )
        unexplained_file_omissions = await self.get_ufos(msg.expected_sensors_folder_prefix)
        print(f"End Readout UFO's: {unexplained_file_omissions}")
