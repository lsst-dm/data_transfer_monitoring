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
from datetime import datetime
from typing import Any, Dict, List


@dataclass(frozen=True, kw_only=True)
class OwnerIdentity:
    principal_id: str


@dataclass(frozen=True, kw_only=True)
class S3Bucket:
    name: str
    owner_identity: OwnerIdentity
    arn: str
    id: str


@dataclass(frozen=True, kw_only=True)
class S3Object:
    key: str
    size: int
    e_tag: str
    version_id: str
    sequencer: str
    metadata: List[Dict[str, Any]] = None
    tags: List[Dict[str, Any]] = None


@dataclass(frozen=True, kw_only=True)
class S3Info:
    s3_schema_version: str
    configuration_id: str
    bucket: S3Bucket
    object: S3Object


@dataclass(frozen=True, kw_only=True)
class UserIdentity:
    principal_id: str


@dataclass(frozen=True, kw_only=True)
class RequestParameters:
    source_ip_address: str


@dataclass(frozen=True, kw_only=True)
class ResponseElements:
    x_amz_request_id: str
    x_amz_id_2: str


@dataclass(frozen=True, kw_only=True)
class Record:
    event_version: str
    event_source: str
    aws_region: str
    event_time: datetime
    event_name: str
    user_identity: UserIdentity
    request_parameters: RequestParameters
    response_elements: ResponseElements
    s3: S3Info
    event_id: str
    opaque_data: str


@dataclass(frozen=True, kw_only=True)
class FileNotificationModel:
    records: List[Record]

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "FileNotificationModel":
        def parse_record(record: Dict[str, Any]) -> Record:
            return Record(
                event_version=record["eventVersion"],
                event_source=record["eventSource"],
                aws_region=record["awsRegion"],
                event_time=datetime.fromisoformat(record["eventTime"]),
                event_name=record["eventName"],
                user_identity=UserIdentity(
                    principal_id=record["userIdentity"]["principalId"]
                ),
                request_parameters=RequestParameters(
                    source_ip_address=record["requestParameters"]["sourceIPAddress"]
                ),
                response_elements=ResponseElements(
                    x_amz_request_id=record["responseElements"]["x-amz-request-id"],
                    x_amz_id_2=record["responseElements"]["x-amz-id-2"]
                ),
                s3=S3Info(
                    s3_schema_version=record["s3"]["s3SchemaVersion"],
                    configuration_id=record["s3"]["configurationId"],
                    bucket=S3Bucket(
                        name=record["s3"]["bucket"]["name"],
                        owner_identity=OwnerIdentity(
                            principal_id=record["s3"]["bucket"]["ownerIdentity"]["principalId"]
                        ),
                        arn=record["s3"]["bucket"]["arn"],
                        id=record["s3"]["bucket"]["id"]
                    ),
                    object=S3Object(
                        key=record["s3"]["object"]["key"],
                        size=record["s3"]["object"]["size"],
                        e_tag=record["s3"]["object"]["eTag"],
                        version_id=record["s3"]["object"]["versionId"],
                        sequencer=record["s3"]["object"]["sequencer"],
                        metadata=record["s3"]["object"].get("metadata", []),
                        tags=record["s3"]["object"].get("tags", [])
                    )
                ),
                event_id=record["eventId"],
                opaque_data=record["opaqueData"]
            )

        return cls(records=[parse_record(r) for r in json_data["Records"]])
