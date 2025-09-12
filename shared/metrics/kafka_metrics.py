import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Set, Any, Optional

from prometheus_client import Counter, Gauge, Histogram

from models.end_readout import EndReadoutModel
from models.file_notification import FileNotificationModel
from shared.notifications.notification_tracker import NotificationTracker
from shared.utils.day_of_observation import get_observation_day
from shared import constants

log = logging.getLogger(__name__)


class KafkaMetrics:
    """Handles all Kafka-related metrics processing and calculations."""

    def __init__(self):
        """Initialize all metrics and internal state variables."""
        self.current_day_obs = ""
        self._total_end_readouts_recieved = 0
        self._total_transfer_seconds = 0
        self._total_end_readouts_resolved = 0
        self._total_end_readouts_orphaned = 0

        # Initialize the notification tracker
        self.notification_tracker = NotificationTracker()

        # Initialize all Prometheus metrics
        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize all Prometheus metrics."""
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

    def increment_expected_files(self, count: int):
        """Increment the total expected files counter."""
        self.total_expected_files.inc(count)

    def increment_end_readouts_received(self, day_obs: str = None):
        """Increment the total end readouts received counter."""
        if day_obs is None:
            day_obs = get_observation_day()
        self._total_end_readouts_recieved += 1
        self.total_end_readouts_received.labels(day=day_obs).inc()

    def increment_end_readouts_resolved(self, count: int, day_obs: str = None):
        """Increment the total end readouts resolved counter."""
        if day_obs is None:
            day_obs = get_observation_day()
        self._total_end_readouts_resolved += count
        self.total_end_readouts_resolved.labels(day=day_obs).inc(count)
        log.info(f"num resolved end readouts: {count}")
        log.info(f"total num resolved end readouts: {self._total_end_readouts_resolved}")

    def increment_end_readouts_orphaned(self):
        """Increment the total end readouts orphaned counter."""
        self._total_end_readouts_orphaned += 1
        log.info(f"total num orphaned end readouts: {self._total_end_readouts_orphaned}")

    def record_transfer_time_metrics(self, data: Tuple):
        """
        Calculates the transfer time for the image.
        Transfer time is the time between when the end readout was complete
        and the timestamp of the last found file.
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

        last_file_to_arrive = sorted_by_time[-1][0].timestamp
        log.info(f"last file to arrive: {last_file_to_arrive}")
        log.info(f"msg timestamp: {msg.timestamp}")
        time_diff = last_file_to_arrive - msg.timestamp

        transfer_seconds = abs(time_diff.total_seconds())
        log.info(f"transfer time: {transfer_seconds} seconds")
        day_obs = get_observation_day(msg.timestamp)
        log.info(f"day_obs={day_obs}")

        # Check if it's a new day obs, if it is then reset counters
        if not day_obs == self.current_day_obs and self.current_day_obs:
            self._total_transfer_seconds = 0
            self._total_end_readouts_recieved = 0
            self.current_day_obs = day_obs

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

    def record_metrics_for_resolved_end_readout(self, end_readout: Tuple):
        """Record metrics for a resolved end readout."""
        _, _, _, late_fits, _, _, late_json, _, _ = end_readout
        self.total_late_files.inc(len(late_fits) + len(late_json))
        self.total_late_fits_files.inc(len(late_fits))
        self.total_late_json_files.inc(len(late_json))
        self.record_transfer_time_metrics(end_readout)

    async def record_metrics_for_orphans(self, orphan_data: List):
        """
        Record metrics for orphaned messages.

        If a file notification gets put in orphans, it is a late file.
        If an end readout gets put in orphans, it is missing one or more file notifications.
        """
        num_end_readout_orphans = len([data for data in orphan_data if isinstance(data[1][0], EndReadoutModel)])
        log.info(f"Number of end readout orphans: {num_end_readout_orphans}")

        # Sort orphan data: end readouts first, then file notifications
        sorted_orphan_data = sorted(
            orphan_data, key=lambda x: not isinstance(x[1][0], EndReadoutModel)
        )

        for key, data in sorted_orphan_data:
            msg = data[0]

            if isinstance(msg, FileNotificationModel):
                await self._process_orphaned_file_notification(key, msg)

            elif isinstance(msg, EndReadoutModel):
                await self._process_orphaned_end_readout(key, data)

            await self.notification_tracker.pop_orphan(key)

        # Log orphan image numbers for debugging
        image_numbers = set()
        for orphan in orphan_data:
            msg = orphan[1][0]
            image_numbers.add(msg.image_number)
        log.info(f"orphan data: {len(orphan_data)}")
        log.info(f"orphan image numbers: {image_numbers}")

    async def _process_orphaned_file_notification(self, key: str, msg: FileNotificationModel):
        """Process metrics for an orphaned file notification."""
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

    async def _process_orphaned_end_readout(self, key: str, data: Tuple):
        """Process metrics for an orphaned end readout."""
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

        # Calculate missing files
        found_fits_fids = [x[0] for x in found_fits]
        missing_fits_files = [x for x in expected_fits if x not in found_fits_fids]

        found_json_fids = [x[0] for x in found_json]
        missing_json_files = [x for x in expected_json if x not in found_json_fids]

        total_missing_files = missing_fits_files + missing_json_files

        log.info(f"incrementing missing by this amount: {len(total_missing_files)}")
        self.total_missing_files.inc(len(total_missing_files))
        self.total_missing_fits_files.inc(len(missing_fits_files))
        self.total_missing_json_files.inc(len(missing_json_files))
        self.total_late_fits_files.inc(len(late_fits))
        self.total_late_json_files.inc(len(late_json))
        self.total_late_files.inc(len(late_json) + len(late_fits))
        self.total_incomplete_end_readouts.inc()

        # Process science vs guider files
        await self._log_incomplete_readout_details(msg, sensors, total_missing_files)

        # Log missing files details
        now = datetime.now(timezone.utc)
        missing_files_contents = await self.notification_tracker.get_missing_files()
        num_missing = len([m for m in total_missing_files if m in missing_files_contents])

        log.info(f"pretty sure missing this many files: {len(total_missing_files)}")
        for file in total_missing_files:
            log.info(f"Missing file {file} as of {now.strftime('%Y-%m-%d %H:%M:%S')} for end readout sequence number: {msg.private_seqNum}")
        log.info(f"Total missing files actually found in the missing bucket: {num_missing}")
        log.info(f"total actually missing: {len([m for m in total_missing_files if m not in missing_files_contents])}")

        await self.notification_tracker.add_missing_files(set(total_missing_files), key)
        self.increment_end_readouts_orphaned()

    async def _log_incomplete_readout_details(self, msg: EndReadoutModel, sensors, total_missing_files: List):
        """Log details about incomplete readouts, separating science and guider files."""
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
