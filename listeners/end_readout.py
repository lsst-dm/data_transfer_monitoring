import logging
import json
from datetime import datetime

from prometheus_client import Counter
from prometheus_client import Gauge

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker

log = logging.getLogger(__name__)


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.notification_tracker = NotificationTracker()

        self.total_expected_files = Counter(
            "dtm_total_expected_files", "Total number of expected files"
        )

        self.total_missing_files = Gauge(
            "dtm_total_missing_files", "Total number of missing files"
        )

        self.total_late_files = Counter(
            "dtm_total_late_files", "Total number of late files"
        )

        self.total_missing_fits_files = Gauge(
            "dtm_total_missing_fits_files", "Total number of missing .fits files"
        )

        self.total_missing_json_files = Gauge(
            "dtm_total_missing_json_files", "Total number of missing .json files"
        )

        self.total_late_fits_files = Counter(
            "dtm_total_late_fits_files", "Total number of late .fits files"
        )

        self.total_late_json_files = Counter(
            "dtm_total_late_json_files", "Total number of late .json files"
        )
        self.total_missing_end_readouts = Gauge(
            "dtm_total_missing_end_readouts", "Total number of missing end readouts"
        )

        self.total_incomplete_end_readouts = Counter(
            "dtm_total_incomplete_end_readouts",
            "Total number of incomplete end readouts",
        )

    async def process_end_readout(self, msg):
        path_prefix = msg.expected_sensors_folder_prefix
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            log.info(f"Did not find expected sensors file for path prefix: {path_prefix}")
            return [], []
        expected_fits_files, expected_json_files = sensors.get_expected_file_keys()
        total_expected_sensors = len(expected_fits_files) + len(expected_json_files)
        self.total_expected_files.inc(total_expected_sensors)

        status = await self.notification_tracker.handle_end_readout(
            sensors.storage_key, expected_fits_files, expected_json_files, msg, sensors
        )

        return status["missing_fits"], status["missing_json"]

    def should_skip(self, msg):
        should_skip = not msg.image_source == "MC"
        if should_skip:
            log.info(f"skipping end readout, image source is: {msg.image_source}")
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
        # need to loop through end readouts first and then file notifications
        sorted_orphan_data = sorted(
            orphan_data, key=lambda x: not isinstance(x[1][0], EndReadoutModel)
        )
        for key, data in sorted_orphan_data:
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
                (
                    msg,
                    expected_fits,
                    found_fits,
                    late_fits,
                    expected_json,
                    found_json,
                    late_json,
                    sensors,
                    _,
                ) = data
                missing_fits_files = expected_fits - found_fits
                missing_json_files = expected_json - found_json
                total_missing_files = missing_fits_files | missing_json_files
                self.total_missing_files.inc(len(total_missing_files))
                self.total_missing_fits_files.inc(len(missing_fits_files))
                self.total_missing_json_files.inc(len(missing_json_files))
                self.total_late_fits_files.inc(len(late_fits))
                self.total_late_json_files.inc(len(late_json))
                self.total_late_files.inc(len(late_json) + len(late_fits))
                self.total_incomplete_end_readouts.inc()

                await self.notification_tracker.add_missing_files(
                    total_missing_files, key
                )

                # come from expectedSensors.json file
                # dayobs=20250513 seqnum=13 expect_s=197 expect_g=0 found_s=75 found_g=0 ingest_s=75 SOME MISSING
                # s = SCIENCE, g = GUIDER
                expected_fits_science, expected_json_science = sensors.get_expected_science_keys()
                all_expected_science = expected_fits_science | expected_json_science
                missing_science = set(key for key in total_missing_files if key in all_expected_science)

                expected_fits_guider, expected_json_guider = sensors.get_expected_guider_keys()
                all_expected_guider = expected_fits_guider | expected_json_guider
                missing_guider = set(key for key in total_missing_files if key in all_expected_guider)

                log_msg = (
                    "incomplete end readout:"
                    f"dayobs={msg.image_date}"
                    f"seqnum={msg.private_seqNum}"
                    f"expect_s={len(all_expected_science)}"
                    f"expect_g={len(all_expected_guider)}"
                    f"found_s={len(all_expected_science - missing_science)}"
                    f"found_g={len(all_expected_guider - missing_guider)}"
                    f"{'SOME MISSING' if len(missing_guider | missing_science) > 0 else 'ALL FOUND'}"
                )

                log.info(log_msg)

                now = datetime.now()
                for file in total_missing_files:
                    log.info(f"Missing file {file} as of {now.strftime('%Y-%m-%d %H:%M:%S')} for end readout sequence number: {msg.private_seqNum}")

            await self.notification_tracker.pop_orphan(key)

        log.info(f"orphan data: {len(orphan_data)}")

    async def handle_message(self, message, deserializer):
        log.info("received end readout message")
        log.debug(f"end readout message json: {message}")
        if deserializer:
            message = await deserializer.deserialize(data=message)
            message = json.dumps(message)
        msg = EndReadoutModel.from_json(message)
        # if self.should_skip(msg):
        #     return

        resolved_end_readouts = (
            await self.notification_tracker.resolve_pending_end_readouts()
        )
        for readout in resolved_end_readouts:
            _, _, _, late_fits, _, _, late_json, _, _ = readout
            self.total_late_files.inc(len(late_fits) + len(late_json))
            self.total_late_fits_files.inc(len(late_fits))
            self.total_late_json_files.inc(len(late_json))

        await self.record_metrics_for_orphans()
        await self.process_end_readout(msg)
