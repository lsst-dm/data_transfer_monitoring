import logging

from prometheus_client import Counter
from prometheus_client import Gauge

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.expected_sensors import ExpectedSensorsModel
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.notification_tracker = NotificationTracker()

        self.total_expected_files = Counter(
            "total_expected_files", "Total number of expected files"
        )

        self.total_missing_files = Gauge(
            "total_missing_files", "Total number of missing files"
        )

        self.total_late_files = Counter(
            "total_late_files", "Total number of late files"
        )

        self.total_missing_fits_files = Gauge(
            "total_missing_fits_files", "Total number of missing .fits files"
        )

        self.total_missing_json_files = Gauge(
            "total_missing_json_files", "Total number of missing .json files"
        )

        self.total_late_fits_files = Counter(
            "total_late_fits_files", "Total number of late .fits files"
        )

        self.total_late_json_files = Counter(
            "total_late_json_files", "Total number of late .json files"
        )
        self.total_missing_end_readouts = Gauge(
            "total_missing_end_readouts", "Total number of missing end readouts"
        )

        self.total_incomplete_end_readouts = Counter(
            "total_incomplete_end_readouts", "Total number of incomplete end readouts"
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

    async def process_end_readout(self, msg):
        path_prefix = msg.expected_sensors_folder_prefix
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            return [], []
        expected_fits_files, expected_json_files = self.get_expected_file_keys(sensors)
        total_expected_sensors = len(expected_fits_files) + len(expected_json_files)
        self.total_expected_files.inc(total_expected_sensors)

        status = await self.notification_tracker.handle_end_readout(
            sensors.storage_key, expected_fits_files, expected_json_files, msg
        )

        return status["missing_fits"], status["missing_json"]

    def should_skip(self, msg):
        should_skip = not msg.image_source == "MC"
        if should_skip:
            return True
        return False

    async def record_metrics_for_orphans(self):
        """
            If a file notification gets put in orphans,
            it is a late file and there could be a number of reasons why.
            It could be a late file,
            its end readout was not sent,
            its end readout was sent but not seen (pod failover)

            If an end readout gets put in orphans,
            it is missing one or more file notifications
        """
        orphan_data = await self.notification_tracker.get_orphans_data()
        for key, data in orphan_data:
            print("key: ", key)
            msg = data[0]
            if isinstance(msg, FileNotificationModel):
                is_missing = await self.notification_tracker.is_missing_file(key)
                if is_missing:
                    await self.notification_tracker.pop_missing_file(key)
                    self.total_missing_files.dec()
                self.total_late_files.inc()
                if msg.file_type == FileNotificationModel.FITS:
                    if is_missing:
                        self.total_missing_fits_files.dec()
                    self.total_late_fits_files.inc()
                if msg.file_type == FileNotificationModel.JSON:
                    if is_missing:
                        self.total_missing_json_files.dec()
                    self.total_late_json_files.inc()
            if isinstance(msg, EndReadoutModel):
                msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, _ = data
                missing_fits_files = expected_fits - found_fits
                missing_json_files = expected_json - found_json
                total_missing_files = missing_fits_files | missing_json_files
                print("missing files: ", total_missing_files)
                self.total_missing_files.inc(len(total_missing_files))
                self.total_missing_fits_files.inc(len(missing_fits_files))
                self.total_missing_json_files.inc(len(missing_json_files))
                self.total_late_fits_files.inc(len(late_fits))
                self.total_late_json_files.inc(len(late_json))
                self.total_late_files.inc(len(late_json) + len(late_fits))
                self.total_incomplete_end_readouts.inc()

                await self.notification_tracker.add_missing_files(total_missing_files)

            await self.notification_tracker.pop_orphan(key)
            
        logging.info("orphan data: ", len(orphan_data))

    async def handle_message(self, message):
        msg = EndReadoutModel.from_json(message)
        if self.should_skip(msg):
            return

        resolved_end_readouts = await self.notification_tracker.resolve_pending_end_readouts()
        for readout in resolved_end_readouts:
            pass
            _, _, _, late_fits, _, _, late_json, _ = readout
            self.total_late_files.inc(len(late_fits) + len(late_json))
            self.total_late_fits_files.inc(len(late_fits))
            self.total_late_json_files.inc(len(late_json))

        await self.record_metrics_for_orphans()
        await self.process_end_readout(msg)
