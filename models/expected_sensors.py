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

from dataclasses import dataclass
from typing import Dict
from typing import Any
import json


@dataclass(frozen=True, kw_only=True)
class ExpectedSensorsModel:
    """Expected Sensors Message"""
    file_name: str
    file_type: str
    obs_id: str
    version: float
    expected_sensors: Dict[str, str]

    def __init__(
                 self,
                 *args,
                 topic="",
                 bootstrap_servers="",
                 group_id="",
                 **kwargs
             ):
        super().__int__(*args, **kwargs, topic, bootstrap_servers, group_id)

    @classmethod
    def from_raw_message(cls, message: dict[str, Any]):
        """Factory creating an ExpectedSensorsModel from an unpacked message.

        Parameters
        ----------
        message : `dict` [`str`]
            A mapping containing message fields.

        Returns
        -------
        model : `ExpectedSensorsModel`
            An object containing the fields in the message.
        """
        return ExpectedSensorsModel(
            file_name=message["fileName"],
            file_type=message["fileType"],
            obs_id=message["obsId"],
            version=message["version"],
            expected_sensors=json.parse(message["expectedSensors"])
        )
