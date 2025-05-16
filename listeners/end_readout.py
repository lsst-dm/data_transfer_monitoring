import logging

from prometheus_client import Counter

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.expected_sensors import ExpectedSensorsModel
from shared.notifications.notification_tracker import NotificationTracker

# file notification is proof that we have the file, is generated after file write
# end readout hsould just check
# TODO need to validate logic of 15 seconds file being late being "missing"
# late files are already a problem
# 3 data structures, 1 for file notifications, 1 for end readouts 1 for orphans


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.notification_tracker = NotificationTracker()

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

    async def process_end_readout(self, path_prefix: str):
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            return [], []
        expected_fits_files, expected_json_files = self.get_expected_file_keys(sensors)

        status = await self.notification_tracker.handle_end_readout(
            sensors.storage_key, expected_fits_files, expected_json_files
        )

        return status["missing_fits"], status["missing_json"]

    def should_skip(self, msg):
        should_skip = not msg.image_source == "MC"
        if should_skip:
            return True
        return False

    async def handle_message(self, message):
        msg = EndReadoutModel.from_json(message)
        if self.should_skip(msg):
            return

        resolved_end_readouts = await self.notification_tracker.resolve_pending_end_readouts()
        logging.info("num resolved pending end readouts: ", resolved_end_readouts)

        pending_end_readouts = await self.notification_tracker.get_pending_end_readouts()
        logging.info("num pending end readouts: ", len(pending_end_readouts))

        orphaned_end_readouts = await self.notification_tracker.get_orphans()
        logging.info("num orphans: ", len(orphaned_end_readouts))

        # parse expected sensors file and make sure all of the files are in the folder
        # if any are missing, then increment counter
        logging.info("got end readout message")
        await self.process_end_readout(
            msg.expected_sensors_folder_prefix
        )
