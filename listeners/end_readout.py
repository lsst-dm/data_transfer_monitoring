import logging
import json
import asyncio
import time
from datetime import datetime
from datetime import timezone

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

from listeners.base_listener import BaseKafkaListener
from models.end_readout import EndReadoutModel
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker
from shared.s3_metrics_processor import S3MetricsProcessor
from shared.utils.day_of_observation import get_observation_day
from shared import constants

log = logging.getLogger(__name__)


class EndReadoutListener(BaseKafkaListener):
    """Class for handling EndReadout event"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.current_day_obs = ""
        self._total_end_readouts_recieved = 0
        self._total_transfer_seconds = 0
        self._total_end_readouts_resolved = 0
        self._total_end_readouts_orphaned = 0

        self.notification_tracker = NotificationTracker()
        self.s3_metrics_processor = S3MetricsProcessor.get_instance()

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
        self.transfer_time_histogram = Histogram(
            "dtm_transfer_duration_seconds",
            "Transfer duration in seconds",
            ["day"],  # add day as a label
            buckets=(0.5, 1, 2.5, 5, 10, 30, 60)
        )
        self.s3_transfer_time_histogram = Histogram(
            "dtm_s3_transfer_duration_seconds",
            "S3 transfer duration in seconds (from EndReadout timestamp to oldest S3 file)",
            ["day"],  # add day as a label
            buckets=(0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
        )
        self.total_transfer_seconds = Counter(
            "dtm_total_transfer_seconds",
            "Total seconds for all end readouts",
            ["day"]
        )
        self.total_end_readouts_received = Counter(
            "dtm_total_end_readouts_received",
            "Total number of end readouts received",
            ["day"]
        )
        self.total_end_readouts_resolved = Counter(
            "dtm_total_end_readouts_resolved",
            "Total number of end readouts resolved",
            ["day"]
        )
        self.average_transfer_time = Gauge(
            "dtm_average_transfer_time",
            "Average Transfer Time in Seconds",
            ["day"]
        )
        self.s3_late_or_missing = Counter(
            "dtm_s3_late_or_missing",
            "Number of files late or missing according to s3",
            ["day"]
        )
        self.s3_late_files = Counter(
            "dtm_s3_late_files",
            "Number of files in S3 that exceeded the late threshold",
            ["day"]
        )

    async def process_end_readout(self, msg):
        path_prefix = msg.expected_sensors_folder_prefix
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            log.info(f"Did not find expected sensors file for path prefix: {path_prefix}")
            return
        expected_fits_files, expected_json_files = sensors.get_expected_file_keys()
        total_expected_sensors = len(expected_fits_files) + len(expected_json_files)
        self.total_expected_files.inc(total_expected_sensors)

        resolved = await self.notification_tracker.handle_end_readout(
            msg.id, expected_fits_files, expected_json_files, msg, sensors
        )

        return resolved

    def should_skip(self, msg):
        should_skip = not msg.image_source == "MC"
        if should_skip:
            log.info(f"skipping end readout, image source is: {msg.image_source}")
            return True
        return False

    def record_transfer_time_metrics(self, data):
        """
        Calculates the transfer time for the image.
        transfer time is the time between when the end readout was complete and the timestamp of the last found file
        """
        log.info("calculating transfer time metrics")
        (
            msg,
            expected_fits_ids,
            found_fits,
            late_fits,
            expected_json_ids,
            found_json,
            late_json,
            expected_sensors,
            timestamp
        ) = data
        found_fits_data = [x[1] for x in found_fits]
        found_json_data = [x[1] for x in found_json]
        found_files = found_fits_data + found_json_data
        sorted_by_time = sorted(found_files, key=lambda x: x[0].timestamp)
        # log.info(f"sorted by time: {sorted_by_time[0]}")
        # need to change the end readout to have an earlier timestamp
        last_file_to_arrive = sorted_by_time[-1][0].timestamp
        log.info(f"last file to arrive: {last_file_to_arrive}")
        log.info(f"msg timestamp: {msg.timestamp}")
        time_diff = last_file_to_arrive - msg.timestamp

        transfer_seconds = abs(time_diff.total_seconds())
        log.info(f"transfer time: {transfer_seconds} seconds")
        day_obs = get_observation_day(msg.timestamp)
        log.info(f"day_obs={day_obs}")
        # check if its a new day obs, if it is then we need to reset the total transfer seconds and total end readouts
        if not day_obs == self.current_day_obs and self.current_day_obs:
            self._total_transfer_seconds = 0
            self._total_end_readouts_recieved = 0

        self.transfer_time_histogram.labels(day=day_obs).observe(transfer_seconds)
        self._total_transfer_seconds += transfer_seconds
        self.total_transfer_seconds.labels(day=day_obs).inc(transfer_seconds)
        average_transfer_time = self._total_transfer_seconds / self._total_end_readouts_resolved
        self.average_transfer_time.labels(day=day_obs).set(average_transfer_time)
        log.info(f"average transfer time internal: {average_transfer_time}")
        log.info(f"total end readouts internal: {self._total_end_readouts_resolved}")
        log.info(f"total transfer seconds internal: {self._total_transfer_seconds}")

        num_end_readouts_resolved = self.total_end_readouts_resolved.labels(day=day_obs)._value.get()
        if num_end_readouts_resolved > 0:
            metric_average_transfer_time = self.total_transfer_seconds.labels(day=day_obs)._value.get() / num_end_readouts_resolved
            log.info(f"average transfer time metric: {metric_average_transfer_time}")
            log.info(f"total end readouts metric: {num_end_readouts_resolved}")
            log.info(f"total transfer seconds metric: {num_end_readouts_resolved}")

        # need to sort found files by timestamp
        # then get the time between  when the end readout was complete and the timestamp of the last found file

    async def record_metrics_for_orphans(self):
        """
        If a file notification gets put in orphans,
        it is a late file and there could be a number of reasons why.
        It could be a late file,
        its end readout was not sent,
        its end readout was sent but not seen (pod failover)

        If an end readout gets put in orphans,
        it is missing one or more file notifications

        the file notification may be missing,
        the file notification may be still in the message queue,

        """
        orphan_data = await self.notification_tracker.get_orphans_data()
        num_end_readout_orphans = len([data for data in orphan_data if isinstance(data[1][0], EndReadoutModel)])
        log.info(f"Number of end readout orphans: {num_end_readout_orphans}")
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
                # missing_fits_files = expected_fits - found_fits
                found_fits_fids = [x[0] for x in found_fits]
                missing_fits_files = [x for x in expected_fits if x not in found_fits_fids]
                # missing_json_files = expected_json - found_json
                found_json_fids = [x[0] for x in found_json]
                missing_json_files = [x for x in expected_json if x not in found_json_fids]
                # total_missing_files = missing_fits_files | missing_json_files
                total_missing_files = missing_fits_files + missing_json_files
                log.info(f"incrementing missing by this amount: {len(total_missing_files)}")
                self.total_missing_files.inc(len(total_missing_files))
                self.total_missing_fits_files.inc(len(missing_fits_files))
                self.total_missing_json_files.inc(len(missing_json_files))
                self.total_late_fits_files.inc(len(late_fits))
                self.total_late_json_files.inc(len(late_json))
                self.total_late_files.inc(len(late_json) + len(late_fits))
                self.total_incomplete_end_readouts.inc()

                # come from expectedSensors.json file
                # dayobs=20250513 seqnum=13 expect_s=197 expect_g=0 found_s=75 found_g=0 ingest_s=75 SOME MISSING
                # s = SCIENCE, g = GUIDER
                expected_fits_science, expected_json_science = sensors.get_expected_science_keys()
                all_expected_science = expected_fits_science | expected_json_science
                missing_science = set(key for key in total_missing_files if key in all_expected_science)

                expected_fits_guider, expected_json_guider = sensors.get_expected_guider_keys()
                all_expected_guider = expected_fits_guider | expected_json_guider
                missing_guider = set(key for key in total_missing_files if key in all_expected_guider)

                is_incomplete = True
                no_missing_science = len(all_expected_science) == len(all_expected_science - missing_science)
                no_missing_guider = len(all_expected_guider) == len(all_expected_guider - missing_guider)
                if no_missing_science and no_missing_guider:
                    is_incomplete = False

                if is_incomplete:
                    log_msg = (
                        "incomplete end readout:"
                        f"dayobs={msg.image_date}"
                        f"seqnum={msg.private_seqNum}"
                        f"expect_s={len(all_expected_science)}"
                        f"expect_g={len(all_expected_guider)}"
                        f"found_s={len(all_expected_science - missing_science)}"
                        f"found_g={len(all_expected_guider - missing_guider)}"
                        "SOME MISSING"
                    )
                    log.info(log_msg)

                now = datetime.now(timezone.utc)
                missing_files_contents = await self.notification_tracker.get_missing_files()
                num_missing = len([m for m in total_missing_files if m in missing_files_contents])
                log.info(f"pretty sure missing this many files: {len(total_missing_files)}")
                for file in total_missing_files:
                    log.info(f"Missing file {file} as of {now.strftime('%Y-%m-%d %H:%M:%S')} for end readout sequence number: {msg.private_seqNum}")
                log.info(f"Total missing files actually found in the missing bucket: {num_missing}")
                log.info(f"total actually missing: {len([m for m in total_missing_files if m not in missing_files_contents])}")

                await self.notification_tracker.add_missing_files(
                    set(total_missing_files), key
                )
                self._total_end_readouts_orphaned += 1

                # orphans should be resolved before this point,
                # their metrics handled by the record_metrics_for_resolved_end_readout method
                # so pretty sure we dont need to do this
                # if len(total_missing_files) == 0:
                #     self.record_transfer_time_metrics(data)


            await self.notification_tracker.pop_orphan(key)
        image_numbers = set()
        for orphan in orphan_data:
            msg = orphan[1][0]
            image_numbers.add(msg.image_number)
        log.info(f"orphan data: {len(orphan_data)}")
        log.info(f"orphan image numbers: {image_numbers}")

    def record_metrics_for_resolved_end_readout(self, end_readout):
        _, _, _, late_fits, _, _, late_json, _, _ = end_readout
        self.total_late_files.inc(len(late_fits) + len(late_json))
        self.total_late_fits_files.inc(len(late_fits))
        self.total_late_json_files.inc(len(late_json))
        self.record_transfer_time_metrics(end_readout)

    async def determine_missing_files_in_s3(self, end_readout):
        await asyncio.sleep(constants.MAX_LATE_FILE_TIME)
        path_prefix = end_readout.expected_sensors_folder_prefix
        existing_files = await self.storage_client.list_files(prefix=path_prefix)
        sensors = await self.storage_client.download_and_parse_expected_sensors_file(
            prefix=path_prefix
        )
        if not sensors:
            log.info(f"Did not find expected sensors file for path prefix: {path_prefix}")
            return
        expected_fits_files, expected_json_files = sensors.get_expected_file_keys()
        all_expected_files = expected_json_files | expected_fits_files
        missing_files = [f for f in all_expected_files if f not in existing_files]
        # log.info(f"first expected file: {list(all_expected_files)[0]}")
        # log.info(f"first missing file: {missing_files[0]}")
        if missing_files:
            day_obs = get_observation_day()
            now = datetime.now(timezone.utc)
            log.info(f"for end readout sequence number: {end_readout.private_seqNum} as of {now.strftime('%Y-%m-%d %H:%M:%S')} s3 late files: {missing_files}")
            self.s3_late_or_missing.labels(day=day_obs).inc(len(missing_files))

    async def record_metrics_from_s3(self, msg):
        """
        Records S3 transfer time metrics by comparing EndReadout timestamp
        with the oldest file timestamp in S3.
        """
        await asyncio.sleep(constants.S3_WAIT_TIME)
        # try:
            # Get timestamps from S3 items using the storage prefix
        timestamps_dict = await self.storage_client.get_item_timestamps(
            prefix=msg.expected_sensors_folder_prefix
        )

        if not timestamps_dict:
            log.warning(f"No S3 items found for prefix: {msg.expected_sensors_folder_prefix}")
            return

        # Find files that are late based on MAX_FILE_LATE_TIME threshold
        late_files = []
        for file_key, file_timestamp in timestamps_dict.items():
            time_diff = file_timestamp - msg.timestamp
            if abs(time_diff.total_seconds()) > constants.MAX_LATE_FILE_TIME:
                late_files.append({
                    'key': file_key,
                    'timestamp': file_timestamp,
                    'delay_seconds': abs(time_diff.total_seconds())
                })

        # Log information about late files and record metrics
        if late_files:
            log.info(f"Found {len(late_files)} late files for {msg.image_name} (threshold: {constants.MAX_LATE_FILE_TIME}s)")
            for late_file in late_files:
                log.info(f"  Late file: {late_file['key']} - delay: {late_file['delay_seconds']:.2f}s")

            # Record the late files metric
            day_obs = get_observation_day(msg.timestamp)
            self.s3_late_files.labels(day=day_obs).inc(len(late_files))

        # Extract just the datetime values and sort from newest to oldest
        timestamps = list(timestamps_dict.values())
        timestamps_sorted = sorted(timestamps)

        # Get the oldest timestamp (last in the sorted list)
        oldest_timestamp = timestamps_sorted[-1]

        # Calculate the time difference
        # This represents how long it took from EndReadout to the oldest S3 file
        time_diff = oldest_timestamp - msg.timestamp
        transfer_seconds = time_diff.total_seconds()

        # Transfer seconds should also be a above 0.
        if transfer_seconds < 0:
            log.warning(f"{msg.image_name} transfer seconds negative {transfer_seconds}.  Review timestamp end of readout")

        # Get the observation day for labeling
        day_obs = get_observation_day(msg.timestamp)

        # Record the metric
        self.s3_transfer_time_histogram.labels(day=day_obs).observe(transfer_seconds)

        log.info(f"{msg.image_name} S3 transfer time: {transfer_seconds:.2f} seconds")
        log.info(f"EndReadout timestamp: {msg.timestamp}")
        log.info(f"{msg.image_name} oldest S3 file timestamp: {oldest_timestamp}")

        # except Exception as e:
            # log.info(f"Error recording S3 metrics for {msg.image_number}: {e}")

    async def handle_message(self, message, deserializer):
        if deserializer:
            message = await deserializer.deserialize(data=message)
            message = message["message"]
        else:
            message = json.loads(message)
        # msg = EndReadoutModel.from_raw_message(message)
        msg = EndReadoutModel.from_dict(message)
        log.info(f"end readout message json: {msg}")
        # if self.should_skip(msg):
        #     return
        await self.s3_metrics_processor.add_task(self.determine_missing_files_in_s3(msg))
        await self.s3_metrics_processor.add_task(self.record_metrics_from_s3(msg))
        self._total_end_readouts_recieved += 1
        day_obs = get_observation_day()
        self.total_end_readouts_received.labels(day=day_obs).inc()
        resolved = await self.process_end_readout(msg)
        resolved_pending_end_readouts = (
            await self.notification_tracker.resolve_pending_end_readouts()
        )
        resolved_orphaned_end_readouts = (
            await self.notification_tracker.try_resolve_orphaned_end_readouts()
        )
        total_resolved_end_readouts = resolved_pending_end_readouts + resolved_orphaned_end_readouts
        if resolved:
            total_resolved_end_readouts.append(resolved)
        log.info(f"num resolved end readouts: {len(total_resolved_end_readouts)}")
        log.info(f"total num resolved end readouts: {self._total_end_readouts_resolved}")
        log.info(f"total num orphaned end readouts: {self._total_end_readouts_orphaned}")
        self._total_end_readouts_resolved += len(total_resolved_end_readouts)
        self.total_end_readouts_resolved.labels(day=day_obs).inc(len(total_resolved_end_readouts))
        for readout in total_resolved_end_readouts:
            self.record_metrics_for_resolved_end_readout(readout)

        await self.record_metrics_for_orphans()
