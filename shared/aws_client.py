import logging
import json
import aioboto3
from typing import List
from typing import Optional
from typing import Dict
from typing import Any

from shared import constants
from shared import config


class AsyncS3Client:
    """Class for interacting with AWS S3 storage"""

    def __init__(self):
        self.endpoint = self.get_endpoint()
        self.session = aioboto3.Session(
            aws_access_key_id=constants.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=constants.AWS_SECRET_ACCESS_KEY,
        )

    def get_endpoint(self):
        if config.IS_PROD == "True":
            return ""
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

    async def contains_expected_sensors_file(
        self,
        bucket_name: str = constants.STORAGE_BUCKET_NAME,
        prefix: Optional[str] = "",
    ) -> bool:
        """
        Returns True if any file in the bucket (optionally with prefix)
        contains 'expectedSensors.json' in its filename.
        """
        print("looking for prefix: ", prefix)
        files = await self.list_files(bucket_name, prefix)
        for file_key in files:
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
        target_file = next(
            (key for key in files if constants.EXPECTED_SENSORS_FILENAME in key), None
        )
        if not target_file:
            return None

        async with self.session.client(
            "s3", endpoint_url=self.endpoint
        ) as s3_client:
            response = await s3_client.get_object(Bucket=bucket_name, Key=target_file)
            content = await response["Body"].read()
            return json.loads(content.decode("utf-8"))

    async def upload_file(
        self, key, json_body=None, bucket_name=constants.STORAGE_BUCKET_NAME
    ):
        """
        Uploads an empty file or a JSON body to the specified S3 key.
        If json_body is provided, uploads it as JSON; otherwise, uploads an empty file.
        """
        body = b""
        content_type = "application/octet-stream"
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            content_type = "application/json"
            logging.info("uploading expected sensors file: ", key)

        async with self.session.client(
            "s3", endpoint_url=self.endpoint
        ) as s3:
            await s3.put_object(
                Bucket=bucket_name, Key=key, Body=body, ContentType=content_type
            )


# Example usage:
# import asyncio
# async def main():
#     client = AsyncS3Client(region_name="us-west-2")
#     files = await client.list_files("my-bucket", prefix="some/folder/")
#     print(files)
# asyncio.run(main())
