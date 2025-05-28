import time
import asyncio

from shared.notifications.stores.memory_notification_store import (
    MemoryNotificationStore,
)
from shared.notifications.stores.abstract_notification_store import (
    AbstractNotificationStore,
)
from shared import constants
from models.file_notification import FileNotificationModel
from models.end_readout import EndReadoutModel


class NotificationTracker:
    # Shared stores (class attributes)
    fits_file_notifications: AbstractNotificationStore = MemoryNotificationStore()
    json_file_notifications: AbstractNotificationStore = MemoryNotificationStore()
    pending_end_readouts: AbstractNotificationStore = MemoryNotificationStore()
    orphans: AbstractNotificationStore = MemoryNotificationStore()
    missing_files: AbstractNotificationStore = MemoryNotificationStore()
    max_file_late_time = constants.MAX_LATE_FILE_TIME

    # For singleton periodic cleanup
    _cleanup_task = None
    _cleanup_lock = asyncio.Lock()
    _cleanup_started = False

    @classmethod
    async def start_periodic_cleanup(cls, interval_seconds=1):
        async with cls._cleanup_lock:
            if cls._cleanup_started:
                return  # Already started
            cls._cleanup_started = True

            async def _periodic_cleanup():
                while True:
                    try:
                        await cls.cleanup()
                    except Exception as e:
                        print(f"Cleanup error: {e}")
                    await asyncio.sleep(interval_seconds)

            cls._cleanup_task = asyncio.create_task(_periodic_cleanup())

    @classmethod
    async def cleanup(cls):
        now = time.time()
        # Move orphaned fits
        fits_items = await cls.fits_file_notifications.items()
        to_orphan_fits = [
            fid for fid, (_, ts) in fits_items if now - ts > cls.max_file_late_time
        ]
        for fid in to_orphan_fits:
            val = await cls.fits_file_notifications.pop(fid)
            await cls.orphans.set(fid, val)
        # Move orphaned json
        json_items = await cls.json_file_notifications.items()
        to_orphan_json = [
            jid for jid, (_, ts) in json_items if now - ts > cls.max_file_late_time
        ]
        for jid in to_orphan_json:
            val = await cls.json_file_notifications.pop(jid)
            await cls.orphans.set(jid, val)
        # Remove expired end_readouts
        end_items = await cls.pending_end_readouts.items()
        expired = [erid for erid, (_, _, _, _, _, _, _, ts) in end_items if now - ts > cls.max_file_late_time]
        for erid in expired:
            val = await cls.pending_end_readouts.pop(erid)
            await cls.orphans.set(erid, val)

    async def add_file_notification(self, file_id, file_notification, file_type):
        now = time.time()
        if file_type == FileNotificationModel.FITS:
            await self.fits_file_notifications.set(file_id, (file_notification, now))
        elif file_type == FileNotificationModel.JSON:
            await self.json_file_notifications.set(file_id, (file_notification, now))
        else:
            raise ValueError(f"Unknown file type: {file_type}")

    async def add_missing_files(self, missing_files: set[str]):
        for missing_file in missing_files:
            await self.missing_files.set(missing_file, True)

    async def handle_end_readout(self, end_readout_id, expected_fits_ids, expected_json_ids, msg):
        now = time.time()
        found_fits, missing_fits, late_fits = [], [], []
        found_json, missing_json, late_json = [], [], []
        for fid in expected_fits_ids:
            val = await self.fits_file_notifications.get(fid)
            if val is not None:
                found_fits.append(fid)
            else:
                val = await self.orphans.get(fid)
                if val is not None:
                    late_fits.append(fid)
                else:
                    missing_fits.append(fid)
        for jid in expected_json_ids:
            val = await self.json_file_notifications.get(jid)
            if val is not None:
                found_json.append(jid)
            else:
                val = await self.orphans.get(jid)
                if val is not None:
                    late_json.append(jid)
                else:
                    missing_json.append(jid)

        # Remove found notifications
        for fid in found_fits:
            await self.fits_file_notifications.pop(fid)
        for jid in found_json:
            await self.json_file_notifications.pop(jid)
        for fid in late_fits:
            await self.orphans.pop(fid)
        for jid in late_json:
            await self.orphans.pop(jid)

        if not missing_fits and not missing_json:
            return {
                "status": EndReadoutModel.COMPLETE,
                "found_fits": found_fits,
                "found_json": found_json,
                "missing_fits": [],
                "missing_json": [],
                "late_fits": late_fits,
                "late_json": late_json
            }
        else:
            await self.pending_end_readouts.set(
                end_readout_id,
                (
                    msg,
                    set(expected_fits_ids),
                    set(found_fits),
                    set(late_fits),
                    set(expected_json_ids),
                    set(found_json),
                    set(late_json),
                    now,
                ),
            )
            return {
                "status": EndReadoutModel.PENDING,
                "found_fits": found_fits,
                "found_json": found_json,
                "missing_fits": missing_fits,
                "missing_json": missing_json,
                "late_fits": late_fits,
                "late_json": late_json
            }

    async def resolve_pending_end_readouts(self):
        """
        For each pending end readout:
        - Check for expected files in the notifications stores.
        - Pop any found files into the found sets.
        - If all expected files are found, remove the end readout from pending.
        Returns a list of resolved end_readout IDs.
        """
        resolved = []
        end_items = await self.pending_end_readouts.items()
        for erid, (
            msg,
            expected_fits,
            found_fits,
            late_fits,
            expected_json,
            found_json,
            late_json,
            ts,
        ) in end_items:
            # Convert to sets in case they're not already (defensive)
            expected_fits = set(expected_fits)
            found_fits = set(found_fits)
            late_fits = set(late_fits)
            expected_json = set(expected_json)
            found_json = set(found_json)
            late_json = set(late_json)

            # Check and pop new found FITS files
            for fid in expected_fits - found_fits:
                val = await self.fits_file_notifications.get(fid)
                if val is not None:
                    await self.fits_file_notifications.pop(fid)
                    found_fits.add(fid)
                else:
                    val = await self.orphans.get(fid)
                    if val is not None:
                        await self.orphans.pop(fid)
                        late_fits.add(fid)
                        found_fits.add(fid)

            # Check and pop new found JSON files
            for jid in expected_json - found_json:
                val = await self.json_file_notifications.get(jid)
                if val is not None:
                    await self.json_file_notifications.pop(jid)
                    found_json.add(jid)
                else:
                    val = await self.orphans.get(jid)
                    if val is not None:
                        await self.orphans.pop(jid)
                        late_json.add(jid)
                        found_json.add(jid)

            # If all expected files are found, resolve this end readout
            if expected_fits == found_fits and expected_json == found_json:
                data = await self.pending_end_readouts.pop(erid)
                resolved.append(data)
            else:
                # Update the found sets in the pending_end_readouts store
                await self.pending_end_readouts.set(
                    erid,
                    (msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, ts)
                )
        return resolved

    async def check_pending_end_readout_for_file_notification(self, file_id, file_type):
        resolved_end_readouts = []
        end_items = await self.pending_end_readouts.items()
        for erid, (
            expected_fits,
            found_fits,
            expected_json,
            found_json,
            ts,
        ) in end_items:
            if (
                file_type == FileNotificationModel.FITS
                and file_id in expected_fits
                and file_id not in found_fits
            ):
                found_fits.add(file_id)
                await self.fits_file_notifications.pop(file_id)
            if (
                file_type == FileNotificationModel.JSON
                and file_id in expected_json
                and file_id not in found_json
            ):
                found_json.add(file_id)
                await self.json_file_notifications.pop(file_id)
            if expected_fits == found_fits and expected_json == found_json:
                await self.pending_end_readouts.pop(erid)
                resolved_end_readouts.append(erid)
        return resolved_end_readouts

    async def add_file_notification_and_check_pending(
        self, file_id, file_notification, file_type
    ):
        await self.add_file_notification(file_id, file_notification, file_type)
        await self.check_pending_end_readout_for_file_notification(file_id, file_type)

    async def get_orphans(self):
        items = await self.orphans.items()
        return [v for k, v in items]

    async def get_orphans_data(self):
        items = await self.orphans.items()
        return items

    async def pop_orphan(self, key):
        orphan = await self.orphans.pop(key)
        return orphan

    async def get_pending_end_readouts(self):
        keys = await self.pending_end_readouts.keys()
        return list(keys)

    async def get_fits_file_notifications(self):
        keys = await self.fits_file_notifications.keys()
        return list(keys)

    async def get_json_file_notifications(self):
        keys = await self.json_file_notifications.keys()
        return list(keys)

    async def get_missing_files(self):
        missing = await self.missing_files.keys()
        return list(missing)

    async def is_missing_file(self, file) -> bool:
        missing_files = await self.get_missing_files()
        return file in missing_files

    async def pop_missing_file(self, file):
        removed_file = await self.missing_files.pop(file)
        return removed_file

