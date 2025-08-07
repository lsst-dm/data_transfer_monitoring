# This file is part of data_transfer_monitoring.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
from dataclasses import dataclass, field
from typing import Dict
from dataclasses_json import dataclass_json, config


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class ExpectedSensorsModel:
    """Expected Sensors Message"""
    # file_name: str
    # file_type: str
    # obs_id: str
    # version: float
    # expected_sensors: Dict[str, str]
    file_name: str = field(metadata=config(field_name="fileName"))
    file_type: str = field(metadata=config(field_name="fileType"))
    obs_id: str = field(metadata=config(field_name="obsId"))
    version: float
    expected_sensors: Dict[str, str] = field(metadata=config(field_name="expectedSensors"))

    SCIENCE = "SCIENCE"
    GUIDER = "GUIDER"

    @classmethod
    def from_raw_file(cls, file):

        return ExpectedSensorsModel(
          file_name=file["fileName"],
          file_type=file["fileType"],
          obs_id=str(file["obsId"]),
          version=float(file["version"]),
          expected_sensors=dict[str,str](file["expectedSensors"]),
        )

    @property
    def storage_key(self):
        img_source, img_controller, img_date, img_num, _ = self.file_name.split("_")
        folder = "_".join([img_source, img_controller, img_date, img_num])
        return os.path.join("LSSTCam", img_date, folder, self.file_name)

    def _make_keys(self, sensors, ext: str):
        image_source, image_controller, image_date, image_number = self.obs_id.split(
            "_"
        )
        expected_sensor_keys = set()

        for sensor, sensor_kind in sensors:
            # if its a guider, then add guider to filename
            if sensor_kind == self.GUIDER:
                sensor_key = f"LSSTCam/{image_date}/{self.obs_id}/{self.obs_id}_{sensor}_{self.GUIDER.lower()}{ext}"
            # otherwise just construct like normal
            else:
                sensor_key = f"LSSTCam/{image_date}/{self.obs_id}/{self.obs_id}_{sensor}{ext}"
            expected_sensor_keys.add(sensor_key)

        return expected_sensor_keys

    def get_expected_file_keys(self):
        # this doesnt take into account guider files, guider files have _guider'
        # appended before the .ext
        sensors = self.expected_sensors.items()
        expected_json_files = self._make_keys(sensors, ".json")
        expected_fits_files = self._make_keys(sensors, ".fits")
        return expected_fits_files, expected_json_files

    def get_expected_science_sensors(self):
        return set((key, value) for key, value in self.expected_sensors.items() if value == self.SCIENCE)

    def get_expected_guider_sensors(self):
        return set((key, value) for key, value in self.expected_sensors.items() if value == self.GUIDER)

    def get_expected_science_keys(self):
        science_sensors = self.get_expected_science_sensors()
        expected_json_files = self._make_keys(science_sensors, ".json")
        expected_fits_files = self._make_keys(science_sensors, ".fits")
        return expected_fits_files, expected_json_files


    def get_expected_guider_keys(self):
        guider_sensors = self.get_expected_guider_sensors()

        expected_json_files = self._make_keys(guider_sensors, ".json")
        expected_fits_files = self._make_keys(guider_sensors, ".fits")
        return expected_fits_files, expected_json_files
