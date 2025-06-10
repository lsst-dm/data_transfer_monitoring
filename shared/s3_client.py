import logging
import json
import aioboto3
from botocore.exceptions import ClientError
from typing import List
from typing import Optional
from typing import Dict
from typing import Any

from shared import constants
from models.expected_sensors import ExpectedSensorsModel

log = logging.getLogger(__name__)


class AsyncS3Client:
    """Class for interacting with AWS S3 storage"""

    def __init__(self):
        self.endpoint = self.get_endpoint()
        self.session = aioboto3.Session(
            aws_access_key_id=constants.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=constants.AWS_SECRET_ACCESS_KEY,
        )

    def get_endpoint(self):
        if constants.IS_PROD == "True":
            return constants.S3_ENDPOINT_URL
        else:
            return "http://localhost:4566"

    async def list_files(
        self,
        bucket_name: str = constants.STORAGE_BUCKET_NAME,
        prefix: Optional[str] = "",
    ) -> List[str]:
        """Asynchronously list all file keys in the specified S3 bucket (optionally with prefix)."""
        keys = []
        async with self.session.client(
            "s3", endpoint_url=self.endpoint
        ) as s3_client:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
        return keys

    async def check_if_key_exists(
        self,
        key: str,
        bucket_name: str = constants.STORAGE_BUCKET_NAME,
    ):
        async with self.session.client("s3", endpoint_url=self.endpoint) as s3_client:
            try:
                await s3_client.head_object(Bucket=bucket_name, Key=key)
                return True
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ('404', 'NoSuchKey'):
                    return False
                else:
                    raise  # Re-raise if it's a different error

    async def contains_expected_sensors_file(
        self,
        bucket_name: str = constants.STORAGE_BUCKET_NAME,
        prefix: Optional[str] = "",
    ) -> bool:
        """
        Returns True if any file in the bucket (optionally with prefix)
        contains 'expectedSensors.json' in its filename.
        """
        files = await self.list_files(bucket_name, prefix)
        for file_key in files:
            log.info(file_key)
            if constants.EXPECTED_SENSORS_FILENAME in file_key:
                return True
        return False

    async def download_and_parse_expected_sensors_file(
        self,
        bucket_name: str = constants.STORAGE_BUCKET_NAME,
        prefix: Optional[str] = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Downloads the first file containing 'expectedSensors.json' in its key,
        parses its content as JSON, and returns the resulting dictionary.
        Returns None if not found.
        """
        files = await self.list_files(bucket_name, prefix)
        log.info(f"files {files}")
        target_file = next(
            (key for key in files if constants.EXPECTED_SENSORS_FILENAME in key), None
        )
        if not target_file:
            log.info(f"S3 client failed to find expected sensors file for bucket: {bucket_name}, prefix: {prefix}")
            return None
        log.info("found expected sensors file")

        async with self.session.client(
            "s3", endpoint_url=self.endpoint
        ) as s3_client:
            response = await s3_client.get_object(Bucket=bucket_name, Key=target_file)
            content = await response["Body"].read()
            log.info(type(content))
            log.info(content)
            return ExpectedSensorsModel.from_raw_file(content)
