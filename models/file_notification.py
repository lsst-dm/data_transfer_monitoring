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
from datetime import datetime
from datetime import timezone
from dataclasses import dataclass, field
from typing import Any, Dict, List
from pathlib import Path
from dataclasses_json import dataclass_json, config


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class OwnerIdentity:
    principal_id: str = field(metadata=config(field_name="principalId"))


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class S3Bucket:
    name: str
    owner_identity: OwnerIdentity = field(metadata=config(field_name="ownerIdentity"))
    arn: str
    id: str


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class S3Object:
    key: str
    size: int
    e_tag: str = field(metadata=config(field_name="eTag"))
    version_id: str = field(metadata=config(field_name="versionId"))
    sequencer: str
    metadata: List[Dict[str, Any]] = None
    tags: List[Dict[str, Any]] = None


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class S3Info:
    s3_schema_version: str = field(metadata=config(field_name="s3SchemaVersion"))
    configuration_id: str = field(metadata=config(field_name="configurationId"))
    bucket: S3Bucket
    object: S3Object


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class UserIdentity:
    principal_id: str = field(metadata=config(field_name="principalId"))


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class RequestParameters:
    source_ip_address: str = field(metadata=config(field_name="sourceIPAddress"))


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class ResponseElements:
    x_amz_request_id: str = field(metadata=config(field_name="x-amz-request-id"))
    x_amz_id_2: str = field(metadata=config(field_name="x-amz-id-2"))


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class Record:
    event_version: str = field(metadata=config(field_name="eventVersion"))
    event_source: str = field(metadata=config(field_name="eventSource"))
    aws_region: str = field(metadata=config(field_name="awsRegion"))
    event_time: str = field(
        metadata=config(
            field_name="eventTime",
        )
    )
    event_name: str = field(metadata=config(field_name="eventName"))
    user_identity: UserIdentity = field(metadata=config(field_name="userIdentity"))
    request_parameters: RequestParameters = field(
        metadata=config(field_name="requestParameters")
    )
    response_elements: ResponseElements = field(
        metadata=config(field_name="responseElements")
    )
    s3: S3Info
    event_id: str = field(metadata=config(field_name="eventId"))
    opaque_data: str = field(metadata=config(field_name="opaqueData"))


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class FileNotificationModel:
    records: List[Record] = field(metadata=config(field_name="Records"))
    JSON = "json"
    FITS = "fits"

    @property
    def file_type(self):
        if self.filepath.suffix == ".json":
            return self.JSON

        return self.FITS

    @property
    def filepath(self):
        return Path(self.records[0].s3.object.key)

    @property
    def storage_key(self):
        return self.records[0].s3.object.key

    @property
    def observation_id(self):
        return self.filepath.parts[2]

    @property
    def image_number(self):
        return self.observation_id

    @property
    def timestamp(self):
        """
            Returns a python utc datetime
        """
        dt = datetime.fromisoformat(self.records[0].event_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt
