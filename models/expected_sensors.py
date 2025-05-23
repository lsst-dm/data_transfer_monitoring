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
    file_name: str = field(metadata=config(field_name="fileName"))
    file_type: str = field(metadata=config(field_name="fileType"))
    obs_id: str = field(metadata=config(field_name="obsId"))
    version: float
    expected_sensors: Dict[str, str] = field(metadata=config(field_name="expectedSensors"))

    @property
    def storage_key(self):
        img_source, img_controller, img_date, img_num, _ = self.file_name.split("_")
        folder = "_".join([img_source, img_controller, img_date, img_num])
        return os.path.join("LSSTCam", img_date, folder, self.file_name)
