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
from dataclasses import dataclass
from typing import Dict
from typing import Any
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class ExpectedSensorsModel:
    """Expected Sensors Message"""
    file_name: str
    file_type: str
    obs_id: str
    version: float
    expected_sensors: Dict[str, str]

    @classmethod
    def from_json(cls, obj: dict[str, Any]):
        """Factory creating an ExpectedSensorsModel from json.

        Parameters
        ----------
        obj: `dict` [`str`]
            A mapping containing fields.

        Returns
        -------
        model : `ExpectedSensorsModel`
            An object containing the fields in the message.
        """
        return ExpectedSensorsModel(
            file_name=obj["fileName"],
            file_type=obj["fileType"],
            obs_id=obj["obsId"],
            version=obj["version"],
            expected_sensors=obj["expectedSensors"]
        )

    @property
    def storage_key(self):
        img_source, img_controller, img_date, img_num, _ = self.file_name.split("_")
        folder = "_".join([img_source, img_controller, img_date, img_num])
        return os.path.join("LSSTCam", img_date, folder, self.file_name)
