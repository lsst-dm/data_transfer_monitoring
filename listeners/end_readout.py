import asyncio
import logging

from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.expected_sensors import ExpectedSensorsModel


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.total_missing_files = Counter(
            "total_missing_files", "Total number of missing files"
        )

        self.total_missing_fits_files = Counter(
            "total_missing_fits_files", "Total number of missing .fits files"
        )

        self.total_missing_json_files = Counter(
            "total_missing_json_files", "Total number of missing .json files"
        )

    def get_expected_file_keys(self, sensors: ExpectedSensorsModel):
        image_source, image_controller, image_date, image_number = sensors.obs_id.split(
            "_"
        )

        expected_json_files = [
            f"LSSTCam/{image_date}/{sensors.obs_id}/{sensors.obs_id}_{sensor}.json"
            for sensor in sensors.expected_sensors.keys()
        ]
        expected_fits_files = [
            f"LSSTCam/{image_date}/{sensors.obs_id}/{sensors.obs_id}_{sensor}.fits"
            for sensor in sensors.expected_sensors.keys()
        ]
        return expected_fits_files, expected_json_files

    async def get_missing_files(self, path_prefix: str):
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            return [], []
        expected_fits_files, expected_json_files = self.get_expected_file_keys(sensors)

        fits_checks = [
            self.storage_client.check_if_key_exists(key) for key in expected_fits_files
        ]
        json_checks = [
            self.storage_client.check_if_key_exists(key) for key in expected_json_files
        ]

        fits_results, json_results = await asyncio.gather(
            asyncio.gather(*fits_checks), asyncio.gather(*json_checks)
        )

        missing_fits_files = [
            key for key, exists in zip(expected_fits_files, fits_results) if not exists
        ]
        missing_json_files = [
            key for key, exists in zip(expected_json_files, json_results) if not exists
        ]

        return missing_fits_files, missing_json_files

    async def handle_message(self, message):
        msg = EndReadoutModel.from_json(message)

        # parse expected sensors file and make sure all of the files are in the folder
        # if any are missing, then increment counter
        logging.info("got end readout message")
        missing_fits_files, missing_json_files = await self.get_missing_files(
            msg.expected_sensors_folder_prefix
        )
        self.total_missing_files.inc(len(missing_fits_files) + len(missing_json_files))
        self.total_missing_fits_files.inc(len(missing_fits_files))
        self.total_missing_json_files.inc(len(missing_json_files))
        logging.info(
            f"End Readout missing files: {len(missing_fits_files) + len(missing_json_files)}"
        )
