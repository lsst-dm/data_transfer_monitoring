import asyncio
import random
from aiokafka import AIOKafkaProducer
import botocore
import json
import logging

from tests.fake_data.data_creator import DataCreator

from shared import constants
from shared.s3_client import AsyncS3Client

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

log = logging.getLogger(__name__)


class Emulator(object):
    def __init__(
        self,
        min_chance_of_late_files: float = 0.001,
        max_chance_of_late_files: float = 0.002,
        min_wait_time: int = 1,
        max_wait_time: int = 5,
        min_late_time: int = 7,
        max_late_time: int = 30,
    ):
        self.min_chance_of_late_files = min_chance_of_late_files
        self.max_chance_of_late_files = max_chance_of_late_files
        self.min_wait_time = min_wait_time
        self.max_wait_time = max_wait_time
        self.min_late_time = min_late_time
        self.max_late_time = max_late_time

        self.total_expected_sensors_uploaded = 0

        self.storage = AsyncS3Client(endpoint="http://localhost:4566")
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self.data_creator = DataCreator()

    async def upload_file(
        self, key, json_body=None, bucket_name=constants.STORAGE_BUCKET_NAME
    ):
        """
        Uploads an empty file or a JSON body to the specified S3 key.
        If json_body is provided, uploads it as JSON; otherwise, uploads an empty file.
        """
        body = b"x"  # LocalStack has issues with empty files, need at least 1 byte
        content_type = "application/octet-stream"
        if json_body is not None:
            body = json.dumps(json_body).encode()
            content_type = "application/json"

        async with self.storage.session.client(
            "s3", endpoint_url=self.storage.endpoint
        ) as s3:
            await s3.put_object(
                Bucket=bucket_name, Key=key, Body=body, ContentType=content_type
            )

    async def ensure_bucket_exists(self):
        async with self.storage.session.client(
            "s3", endpoint_url=self.storage.endpoint
        ) as s3:
            try:
                await s3.head_bucket(Bucket=constants.STORAGE_BUCKET_NAME)
                log.info(f"Bucket '{constants.STORAGE_BUCKET_NAME}' exists.")
            except botocore.exceptions.ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "404":
                    log.info(
                        f"Bucket '{constants.STORAGE_BUCKET_NAME}' does not exist. Creating it..."
                    )
                    await s3.create_bucket(
                        Bucket=constants.STORAGE_BUCKET_NAME,
                    )
                elif error_code == "BucketAlreadyOwnedByYou":
                    log.info(
                        f"Bucket '{constants.STORAGE_BUCKET_NAME}' already exists and is owned by you. Proceeding."
                    )
                else:
                    raise  # re-raise if it's a different error

    async def initialize(self):
        await self.ensure_bucket_exists()
        await self.producer.start()

    def split_late_files(self, files):
        late_files = []
        files_to_send = files.copy()
        chance_of_late_files = random.uniform(
            self.min_chance_of_late_files, self.max_chance_of_late_files
        )
        rand_num = random.random()
        should_send_late_files = rand_num < chance_of_late_files
        if should_send_late_files and len(files) > 0:
            log.info("sending late files")
            num_late = random.randint(0, len(files))
            late_files = random.sample(files, k=num_late)
            # Remove late files from files_to_send
            files_to_send = [f for f in files if f not in late_files]

        return files_to_send, late_files

    async def send_file_notifications(self, files):
        """
            Sends file notifications for the provided files
        """
        async def notify(file_obj):
            msg = file_obj.to_json().encode()
            await self.producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)

        await asyncio.gather(*(notify(f) for f in files))

    async def upload_files_and_confirm(self, files):
        """
        Upload files in parallel, confirm existence in parallel, then send notifications in parallel.
        """
        async def upload(file_obj):
            key = file_obj.records[0].s3.object.key
            await self.upload_file(key)
            return file_obj, key

        upload_results = await asyncio.gather(*(upload(f) for f in files))

        async def confirm(file_obj, key):
            for _ in range(5):
                exists = await self.storage.check_if_key_exists(key=key)
                if exists:
                    return file_obj
                await asyncio.sleep(0.2)
            raise Exception(f"File {key} did not appear in storage after upload!")

        confirmed_files = await asyncio.gather(*(confirm(f, k) for f, k in upload_results))

        await self.send_file_notifications(confirmed_files)

    async def upload_files_and_send_notifications(self, files):
        log.info("uploading files")
        for file_obj in files:
            key = file_obj.records[0].s3.object.key
            await self.upload_file(key)
            msg = file_obj.to_json().encode()
            await self.producer.send_and_wait(
                constants.FILE_NOTIFICATION_TOPIC_NAME, msg
            )

    async def produce_fake_data(self):
        async def send_late_files(late_files):
            # Wait up to 7 seconds before sending late file notifications
            await asyncio.sleep(random.randint(self.min_late_time, self.max_late_time))

            # Upload all late files in parallel first
            upload_tasks = []
            for file_obj in late_files:
                key = file_obj.records[0].s3.object.key
                upload_tasks.append(self.upload_file(key))
            if upload_tasks:
                await asyncio.gather(*upload_tasks)

            # Then send notifications for all late files
            notification_tasks = []
            for file_obj in late_files:
                msg = file_obj.to_json().encode()
                notification_tasks.append(
                    self.producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)
                )
            if notification_tasks:
                await asyncio.gather(*notification_tasks)

        try:
            while True:
                expected_sensors, all_files, end_readout = self.data_creator.create_fake_data()

                # Decide if we will send some files late
                files_to_send, late_files = self.split_late_files(all_files)

                # Upload expected sensors file
                await self.upload_file(
                    expected_sensors.storage_key, json_body=expected_sensors.to_json()
                )
                self.total_expected_sensors_uploaded += 1
                log.info(f"total expected sensors uploaded: {self.total_expected_sensors_uploaded}")

                # Upload all on-time files in parallel
                upload_tasks = []
                for file_obj in files_to_send:
                    key = file_obj.records[0].s3.object.key
                    upload_tasks.append(self.upload_file(key))
                if upload_tasks:
                    await asyncio.gather(*upload_tasks)

                # Prepare on-time file notifications and end readout
                notifications = []
                for file_obj in files_to_send:
                    msg = file_obj.to_json().encode()
                    notifications.append(("file", msg))
                end_readout_msg = end_readout.to_json().encode()
                notifications.append(("end_readout", end_readout_msg))

                # Shuffle and send on-time notifications and end readout
                random.shuffle(notifications)
                for notif_type, msg in notifications:
                    if notif_type == "file":
                        await self.producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)
                    elif notif_type == "end_readout":
                        await self.producer.send_and_wait(constants.END_READOUT_TOPIC_NAME, msg)

                # Spawn a task to send late file notifications after 7 seconds
                if late_files:
                    log.info("sending late files")
                    asyncio.create_task(send_late_files(late_files))

                await asyncio.sleep(
                    random.randint(self.min_wait_time, self.max_wait_time)
                )  # Adjust as needed

        except KeyboardInterrupt:
            await self.producer.stop()
        finally:
            await self.producer.stop()
