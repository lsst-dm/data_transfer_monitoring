from datetime import datetime, timezone
from datetime import timedelta
import asyncio
import logging

from shared.notifications.stores.memory_notification_store import (
    MemoryNotificationStore,
)
from shared.notifications.stores.abstract_notification_store import (
    AbstractNotificationStore,
)
from shared import constants
from models.file_notification import FileNotificationModel
from models.end_readout import EndReadoutModel

log = logging.getLogger(__name__)


class NotificationTracker:
    # Shared stores (class attributes)
    fits_file_notifications: AbstractNotificationStore = MemoryNotificationStore()
    json_file_notifications: AbstractNotificationStore = MemoryNotificationStore()
    pending_end_readouts: AbstractNotificationStore = MemoryNotificationStore()
    orphans: AbstractNotificationStore = MemoryNotificationStore()
    missing_files: AbstractNotificationStore = MemoryNotificationStore()
    max_file_late_time = timedelta(seconds=constants.MAX_LATE_FILE_TIME)

    _data_access_lock = asyncio.Lock()

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
                        log.error(f"Cleanup error: {e}")
                    await asyncio.sleep(interval_seconds)

            cls._cleanup_task = asyncio.create_task(_periodic_cleanup())

    @classmethod
    async def cleanup(cls):
        now = datetime.now(timezone.utc)
        async with cls._data_access_lock:
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
            expired = [erid for erid, (_, _, _, _, _, _, _, _, ts) in end_items if now - ts > cls.max_file_late_time]
            for erid in expired:
                val = await cls.pending_end_readouts.pop(erid)
                await cls.orphans.set(erid, val)

    async def _add_file_notification(self, file_id, file_notification, file_type):
        """Internal version that assumes lock is already held."""
        if file_type == FileNotificationModel.FITS:
            await self.fits_file_notifications.set(file_id, (file_notification, file_notification.timestamp))
        elif file_type == FileNotificationModel.JSON:
            await self.json_file_notifications.set(file_id, (file_notification, file_notification.timestamp))
        else:
            raise ValueError(f"Unknown file type: {file_type}")

    async def add_file_notification(self, file_id, file_notification, file_type):
        """External interface for add_file_notification that acquires the lock."""
        async with self._data_access_lock:
            return await self._add_file_notification(file_id, file_notification, file_type)

    async def _add_file_notifications(self, file_ids, file_notifications, file_types):
        """Internal version that assumes lock is already held."""
        for file_id, file_notification, file_type in zip(file_ids, file_notifications, file_types):
            await self._add_file_notification(file_id, file_notification, file_type)

    async def add_file_notifications(self, file_ids, file_notifications, file_types):
        """External interface for add_file_notifications that acquires the lock."""
        async with self._data_access_lock:
            return await self._add_file_notifications(file_ids, file_notifications, file_types)

    async def _add_missing_files(self, missing_files: set[str], end_readout: str):
        """Internal version that assumes lock is already held."""
        for missing_file in missing_files:
            await self.missing_files.set(missing_file, end_readout)

    async def add_missing_files(self, missing_files: set[str], end_readout: str):
        """External interface for add_missing_files that acquires the lock."""
        async with self._data_access_lock:
            return await self._add_missing_files(missing_files, end_readout)

    async def _handle_end_readout(self, end_readout_id, expected_fits_ids, expected_json_ids, msg, expected_sensors):
        """Internal version that assumes lock is already held."""
        found_fits, missing_fits, late_fits = [], [], []
        found_json, missing_json, late_json = [], [], []
        for fid in expected_fits_ids:
            val = await self.fits_file_notifications.get(fid)
            if val is not None:
                data = (fid, val)
                found_fits.append(data)
            else:
                val = await self.orphans.get(fid)
                if val is not None:
                    data = (fid, val)
                    late_fits.append(data)
                else:
                    val = await self.missing_files.get(fid)
                    if val is not None:
                        data = (fid, val)
                        late_fits.append(data)
                        missing_fits.remove(data)
                    else:
                        data = (fid, None)
                        missing_fits.append(data)
        for jid in expected_json_ids:
            val = await self.json_file_notifications.get(jid)
            if val is not None:
                data = (jid, val)
                found_json.append(data)
            else:
                val = await self.orphans.get(jid)
                if val is not None:
                    data = (jid, val)
                    late_json.append(data)
                else:
                    val = await self.missing_files.get(jid)
                    if val is not None:
                        data = (jid, val)
                        late_json.append(data)
                        missing_json.remove(data)
                    else:
                        data = (jid, None)
                        missing_json.append(data)

        # Remove found notifications
        for fid, val in found_fits:
            await self.fits_file_notifications.pop(fid)
        for jid, val in found_json:
            await self.json_file_notifications.pop(jid)
        for fid, val in late_fits:
            await self.orphans.pop(fid)
        for jid, val in late_json:
            await self.orphans.pop(jid)

        data = (
            msg,
            expected_fits_ids,
            found_fits,
            late_fits,
            expected_json_ids,
            found_json,
            late_json,
            expected_sensors,
            msg.timestamp
        )

        if not missing_fits and not missing_json:
            return data
        else:
            await self.pending_end_readouts.set(
                end_readout_id,
                data,
            )

    async def handle_end_readout(self, end_readout_id, expected_fits_ids, expected_json_ids, msg, expected_sensors):
        """External interface for handle_end_readout that acquires the lock."""
        async with self._data_access_lock:
            return await self._handle_end_readout(end_readout_id, expected_fits_ids, expected_json_ids, msg, expected_sensors)

    async def _resolve_pending_end_readouts(self):
        """Internal version that assumes lock is already held."""
        resolved = []
        end_items = await self.pending_end_readouts.items()
        for erid, end_readout in end_items:
            data = await self._try_resolve_end_readout(end_readout, erid)
            if data:
                resolved.append(data)

        return resolved

    async def resolve_pending_end_readouts(self):
        """
        For each pending end readout:
        - Check for expected files in the notifications stores.
        - Pop any found files into the found sets.
        - If all expected files are found, remove the end readout from pending.
        Returns a list of resolved end_readout IDs.
        """
        async with self._data_access_lock:
            return await self._resolve_pending_end_readouts()

    async def _try_resolve_end_readout(self, end_readout, erid):
        msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, expected_sensors, ts = end_readout

        # Check and pop new found FITS files
        fits_ids = [x[0] for x in found_fits]
        missing_fits = [x for x in expected_fits if x not in fits_ids]
        for fid in missing_fits:
            val = await self.fits_file_notifications.get(fid)
            if val is not None:
                data = (fid, val)
                await self.fits_file_notifications.pop(fid)
                found_fits.append(data)
            else:
                val = await self.orphans.get(fid)
                if val is not None:
                    data = (fid, val)
                    await self.orphans.pop(fid)
                    late_fits.append(data)
                    found_fits.append(data)
                else:
                    val = await self.missing_files.get(fid)
                    if val is not None:
                        data = (fid, val)
                        await self.missing_files.pop(fid)
                        late_fits.append(data)
                        found_fits.append(data)

        # Check and pop new found JSON files
        json_ids = [x[0] for x in found_json]
        missing_json = [x for x in expected_json if x not in json_ids]
        for jid in missing_json:
            val = await self.json_file_notifications.get(jid)
            if val is not None:
                data = (jid, val)
                await self.json_file_notifications.pop(jid)
                found_json.append(data)
            else:
                val = await self.orphans.get(jid)
                if val is not None:
                    data = (jid, val)
                    await self.orphans.pop(jid)
                    late_json.append(data)
                    found_json.append(data)
                else:
                    val = await self.missing_files.get(jid)
                    if val is not None:
                        data = (jid, val)
                        await self.missing_files.pop(jid)
                        late_json.append(data)
                        found_json.append(data)

        # If all expected files are found, resolve this end readout
        if expected_fits == found_fits and expected_json == found_json:
            data = await self.pending_end_readouts.pop(erid)
            return data
        else:
            # Update the found sets in the pending_end_readouts store
            await self.pending_end_readouts.set(
                erid,
                (msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, expected_sensors, ts)
            )

    async def try_resolve_end_readout(self, end_readout, erid):
        """External interface for try_resolve_end_readout that acquires the lock."""
        async with self._data_access_lock:
            return await self._try_resolve_end_readout(end_readout, erid)

    async def _try_resolve_orphaned_end_readout(self, end_readout, erid):
        """Internal version that assumes lock is already held."""
        msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, expected_sensors, ts = end_readout
        files_in_late_bucket = await self._get_missing_files()
        files_in_orphans_bucket = await self._get_orphans()
        fits_files = await self._get_fits_file_notifications()
        json_files = await self._get_json_file_notifications()
        all_files = fits_files + json_files

        # Convert to sets in case they're not already (defensive)
        # expected_fits = set(expected_fits)
        # found_fits = set(found_fits)
        # late_fits = set(late_fits)
        # expected_json = set(expected_json)
        # found_json = set(found_json)
        # late_json = set(late_json)

        # missing_fits = expected_fits - found_fits - late_fits
        found_fits_ids = [x[0] for x in found_fits]
        late_fits_ids = [x[0] for x in late_fits]
        missing_fits = [x for x in expected_fits if x not in found_fits_ids and x not in late_fits_ids]
        # missing_json = expected_json - found_json - late_json
        found_json_ids = [x[0] for x in found_json]
        late_json_ids = [x[0] for x in late_json]
        missing_json = [x for x in expected_json if x not in found_json_ids and x not in late_json_ids]
        # all_missing = missing_fits | missing_json

        # log.info(f"total actually missing in try resolve looking in missing: {len([m for m in all_missing if m not in files_in_late_bucket])}")
        # log.info(f"total actually missing in try resolve looking in orphans: {len([m for m in all_missing if m not in files_in_orphans_bucket])}")
        # log.info(f"total actually missing in try resolve looking in regular_files: {len([m for m in all_missing if m not in all_files])}")
        # Check and pop new found FITS files
        for fid in missing_fits:
            val = await self.fits_file_notifications.get(fid)
            if val is not None:
                data = (fid, val)
                await self.fits_file_notifications.pop(fid)
                found_fits.append(data)
            else:
                val = await self.orphans.get(fid)
                if val is not None:
                    data = (fid, val)
                    await self.orphans.pop(fid)
                    late_fits.append(data)
                    found_fits.append(data)
                else:
                    val = await self.missing_files.get(fid)
                    if val is not None:
                        data = (fid, val)
                        await self.missing_files.pop(fid)
                        late_fits.append(data)
                        found_fits.append(data)

        # Check and pop new found JSON files
        for jid in missing_json:
            val = await self.json_file_notifications.get(jid)
            if val is not None:
                data = (jid, val)
                await self.json_file_notifications.pop(jid)
                found_json.append(data)
            else:
                val = await self.orphans.get(jid)
                if val is not None:
                    data = (jid, val)
                    await self.orphans.pop(jid)
                    late_json.append(data)
                    found_json.append(data)
                else:
                    val = await self.missing_files.get(jid)
                    if val is not None:
                        await self.missing_files.pop(jid)
                        late_json.append(jid)
                        found_json.append(jid)

        # If all expected files are found, resolve this end readout
        if expected_fits == found_fits and expected_json == found_json:
            log.info("resolved orphaned end readout")
            resolved_end_readout = await self.orphans.pop(erid)
            return resolved_end_readout
        else:
            # Update the found sets in the pending_end_readouts store
            await self.orphans.set(
                erid,
                (msg, expected_fits, found_fits, late_fits, expected_json, found_json, late_json, expected_sensors, ts)
            )


    async def try_resolve_orphaned_end_readout(self, end_readout, erid):
        """External interface for try_resolve_orphaned_end_readout that acquires the lock."""
        async with self._data_access_lock:
            return await self._try_resolve_orphaned_end_readout(end_readout, erid)

    async def _try_resolve_orphaned_end_readouts(self):
        """Internal version that assumes lock is already held."""
        resolved = []
        items = await self.orphans.items()
        end_readout_orphans = [o for o in items if isinstance(o[1][0], EndReadoutModel)]
        for erid, end_readout in end_readout_orphans:
            res = await self._try_resolve_orphaned_end_readout(end_readout, erid)
            if res:
                resolved.append(res)
        return resolved

    async def try_resolve_orphaned_end_readouts(self):
        """External interface for try_resolve_orphaned_end_readouts that acquires the lock."""
        async with self._data_access_lock:
            return await self._try_resolve_orphaned_end_readouts()

    async def _get_orphans(self):
        """Internal version that assumes lock is already held."""
        items = await self.orphans.items()
        return [v for k, v in items]

    async def get_orphans(self):
        """External interface for get_orphans that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_orphans()

    async def _get_orphan_by_id(self, id):
        """Internal version that assumes lock is already held."""
        ret = await self.orphans.get(id)
        if ret:
            _, data = ret
            return data

    async def get_orphan_by_id(self, id):
        """External interface for get_orphans that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_orphan_by_id(id)

    async def _get_orphans_data(self):
        """Internal version that assumes lock is already held."""
        items = await self.orphans.items()
        return items

    async def get_orphans_data(self):
        """External interface for get_orphans_data that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_orphans_data()

    async def _pop_orphan(self, key):
        """Internal version that assumes lock is already held."""
        orphan = await self.orphans.pop(key)
        return orphan

    async def pop_orphan(self, key):
        """External interface for pop_orphan that acquires the lock."""
        async with self._data_access_lock:
            return await self._pop_orphan(key)

    async def _get_pending_end_readouts(self):
        """Internal version that assumes lock is already held."""
        keys = await self.pending_end_readouts.keys()
        return list(keys)

    async def get_pending_end_readouts(self):
        """External interface for get_pending_end_readouts that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_pending_end_readouts()

    async def _get_fits_file_notifications(self):
        """Internal version that assumes lock is already held."""
        keys = await self.fits_file_notifications.keys()
        return list(keys)

    async def get_fits_file_notifications(self):
        """External interface for get_fits_file_notifications that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_fits_file_notifications()

    async def _get_json_file_notifications(self):
        """Internal version that assumes lock is already held."""
        keys = await self.json_file_notifications.keys()
        return list(keys)

    async def get_json_file_notifications(self):
        """External interface for get_json_file_notifications that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_json_file_notifications()

    async def _get_missing_files(self):
        """Internal version that assumes lock is already held."""
        missing = await self.missing_files.keys()
        return list(missing)

    async def get_missing_files(self):
        """External interface for get_missing_files that acquires the lock."""
        async with self._data_access_lock:
            return await self._get_missing_files()

    async def _is_missing_file(self, file) -> bool:
        """Internal version that assumes lock is already held."""
        missing_files = await self.missing_files.keys()
        return file in missing_files

    async def is_missing_file(self, file) -> bool:
        """External interface for is_missing_file that acquires the lock."""
        async with self._data_access_lock:
            return await self._is_missing_file(file)

    async def _pop_missing_file(self, file):
        """Internal version that assumes lock is already held."""
        removed_file = await self.missing_files.pop(file)
        return removed_file

    async def pop_missing_file(self, file):
        """External interface for pop_missing_file that acquires the lock."""
        async with self._data_access_lock:
            return await self._pop_missing_file(file)
