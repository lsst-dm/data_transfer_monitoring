import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from prometheus_client import Counter, Histogram

from models.end_readout import EndReadoutModel
from shared.s3_client import AsyncS3Client
from shared.utils.day_of_observation import get_observation_day
from shared import constants

log = logging.getLogger(__name__)


class S3Metrics:
    """Handles all S3-specific metrics calculations and Prometheus metrics."""

    def __init__(self):
        """Initialize S3-specific Prometheus metrics."""
        self.storage_client = AsyncS3Client()

        self.s3_transfer_time_histogram = Histogram(
            "dtm_s3_transfer_duration_seconds",
            "S3 transfer duration in seconds (from EndReadout timestamp to oldest S3 file)",
            ["day"],  # add day as a label
            buckets=(3, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 45, 60)
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

    async def determine_missing_files_in_s3(self, end_readout: EndReadoutModel, sensors=None):
        """
        Determine which files are missing in S3 after the late threshold.

        This method waits for MAX_LATE_FILE_TIME and then checks S3 to see
        which expected files are still missing or late.

        Args:
            end_readout: The EndReadout model containing the expected sensors folder prefix
            storage_client: Client for accessing S3 storage
            sensors: Optional pre-loaded sensors data to avoid re-fetching
        """
        await asyncio.sleep(constants.MAX_LATE_FILE_TIME)
        path_prefix = end_readout.expected_sensors_folder_prefix
        existing_files = await self.storage_client.list_files(prefix=path_prefix)

        if sensors is None:
            sensors = await self.storage_client.download_and_parse_expected_sensors_file(
                prefix=path_prefix
            )

        if not sensors:
            log.info(f"Did not find expected sensors file for path prefix: {path_prefix}")
            return

        expected_fits_files, expected_json_files = sensors.get_expected_file_keys()
        all_expected_files = expected_json_files | expected_fits_files
        missing_files = [f for f in all_expected_files if f not in existing_files]

        if missing_files:
            day_obs = get_observation_day()
            now = datetime.now(timezone.utc)
            log.info(
                f"For Sequence: {end_readout.image_name} "
                f"as of {now.strftime('%Y-%m-%d %H:%M:%S')} "
                f"s3 late files: {missing_files}"
            )

            all_expected_science = sensors.get_expected_science_sensors()
            all_expected_guider = sensors.get_expected_guider_sensors()

            missing_science = set([sensor for sensor in all_expected_science if sensor not in existing_files])
            missing_guider = set([sensor for sensor in all_expected_guider if sensor not in existing_files])
            log_msg = (
                "incomplete end readout:"
                f" dayobs = {get_observation_day()}"
                f" image_name = {end_readout.image_name}"
                f" expect_s={len(all_expected_science)}"
                f" expect_g={len(all_expected_guider)}"
                f" found_s={len(all_expected_science - missing_science)}"
                f" found_g={len(all_expected_guider - missing_guider)}"
                " SOME MISSING"
            )
            log.info(log_msg)

            self.s3_late_or_missing.labels(day=day_obs).inc(len(missing_files))

    async def record_metrics_from_s3(self, msg: EndReadoutModel):
        """
        Records S3 transfer time metrics by comparing EndReadout timestamp
        with the oldest file timestamp in S3.

        Args:
            msg: The EndReadout model containing image information and timestamps
            storage_client: Client for accessing S3 storage
        """
        await asyncio.sleep(constants.S3_WAIT_TIME)

        # Get timestamps from S3 items using the storage prefix
        timestamps_dict = await self.storage_client.get_item_timestamps(
            prefix=msg.expected_sensors_folder_prefix
        )

        if not timestamps_dict:
            log.warning(f"No S3 items found for prefix: {msg.expected_sensors_folder_prefix}")
            return

        # Process and record late files
        self._process_late_files(timestamps_dict, msg)

        # Calculate and record transfer time
        self._calculate_transfer_time(timestamps_dict, msg)

    def _process_late_files(self, timestamps_dict: Dict, msg: EndReadoutModel):
        """
        Find and record metrics for files that are late based on MAX_FILE_LATE_TIME threshold.

        Args:
            timestamps_dict: Dictionary mapping file keys to their timestamps
            msg: The EndReadout model containing the reference timestamp
        """
        late_files = []
        for file_key, file_timestamp in timestamps_dict.items():
            time_diff = file_timestamp - msg.timestamp
            delay_seconds = time_diff.total_seconds()

            if delay_seconds > constants.MAX_LATE_FILE_TIME:
                late_files.append({
                    'key': file_key,
                    'timestamp': file_timestamp,
                    'delay_seconds': delay_seconds
                })

        # Log information about late files and record metrics
        if late_files:
            log.info(
                f"Found {len(late_files)} late files for {msg.image_name} "
                f"(threshold: {constants.MAX_LATE_FILE_TIME}s)"
            )
            for late_file in late_files:
                log.info(
                    f"  Late file: {late_file['key']} - "
                    f"delay: {late_file['delay_seconds']:.2f}s"
                )

            # Record the late files metric
            day_obs = get_observation_day(msg.timestamp)
            self.s3_late_files.labels(day=day_obs).inc(len(late_files))

    def _calculate_transfer_time(self, timestamps_dict: Dict, msg: EndReadoutModel):
        """
        Calculate the S3 transfer time from EndReadout to the oldest S3 file.

        Args:
            timestamps_dict: Dictionary mapping file keys to their timestamps
            msg: The EndReadout model containing the reference timestamp
        """
        # Extract and sort timestamps
        timestamps = list(timestamps_dict.values())
        timestamps_sorted = sorted(timestamps)

        # Get the newest timestamp (last in the sorted list)
        newest_timestamp = timestamps_sorted[-1]

        # Calculate the time difference
        time_diff = newest_timestamp - msg.timestamp
        transfer_seconds = time_diff.total_seconds()

        # Validate transfer time
        if transfer_seconds < 0:
            log.warning(
                f"{msg.image_name} transfer seconds negative {transfer_seconds}. "
                f"Review timestamp end of readout"
            )

        # Get the observation day for labeling
        day_obs = get_observation_day(msg.timestamp)

        # Limit recording of metrics to positive transfers
        # TODO add handling for minimum number of files in a sequence
        if transfer_seconds > 0:
            # Record the metric
            self.s3_transfer_time_histogram.labels(day=day_obs).observe(transfer_seconds)

        log.info(f"{msg.image_name} Summit to USDF transfer time: {transfer_seconds:.2f} seconds")
        log.info(f"{msg.image_name} end readout timestamp: {msg.timestamp}")
        log.info(f"{msg.image_name} newest S3 file timestamp: {newest_timestamp}")
