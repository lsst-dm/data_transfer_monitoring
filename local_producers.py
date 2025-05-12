import logging
import asyncio
import random
from aiokafka import AIOKafkaProducer
import botocore

from tests.fake_data.create_fake_data import create_fake_data

from shared import constants
from shared.aws_client import AsyncS3Client

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"


async def ensure_bucket_exists(session):
    async with session.client(
        "s3", endpoint_url="http://localhost:4566"
    ) as s3:
        try:
            await s3.head_bucket(Bucket=constants.STORAGE_BUCKET_NAME)
            logging.info(f"Bucket '{constants.STORAGE_BUCKET_NAME}' exists.")
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logging.info(
                    f"Bucket '{constants.STORAGE_BUCKET_NAME}' does not exist. Creating it..."
                )
                await s3.create_bucket(
                    Bucket=constants.STORAGE_BUCKET_NAME,
                )
            elif error_code == "BucketAlreadyOwnedByYou":
                logging.info(
                    f"Bucket '{constants.STORAGE_BUCKET_NAME}' already exists and is owned by you. Proceeding."
                )
            else:
                raise  # re-raise if it's a different error


async def produce_fake_data():
    storage_client = AsyncS3Client()
    await ensure_bucket_exists(storage_client.session)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    try:
        while True:
            expected_sensors, all_files, end_readout = create_fake_data()

            # Decide if we will send some files late
            late_files = []
            files_to_send = all_files.copy()
            if random.random() < 0.1 and len(all_files) > 0:  # 10% chance
                num_late = random.randint(1, min(3, len(all_files)))
                late_files = random.sample(all_files, k=num_late)
                # Remove late files from files_to_send
                files_to_send = [f for f in all_files if f not in late_files]

            # Upload and send notifications for files (excluding late ones)
            logging.info("uploading files")
            for file_obj in files_to_send:
                key = file_obj.records[0].s3.object.key
                await storage_client.upload_file(key)
                msg = file_obj.to_json().encode("utf-8")
                await producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)

            # Upload expected sensors file
            await storage_client.upload_file(
                expected_sensors.storage_key,
                json_body=expected_sensors.to_json()
            )

            # Send end readout event
            msg = end_readout.to_json().encode("utf-8")
            await producer.send_and_wait(constants.END_READOUT_TOPIC_NAME, msg)

            # Now send the late file notifications
            for file_obj in late_files:
                key = file_obj.records[0].s3.object.key
                await storage_client.upload_file(key)
                msg = file_obj.to_json().encode("utf-8")
                await producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)
                logging.info(f"Late file notification sent for: {key}")

            await asyncio.sleep(random.randint(5, 7))  # Adjust as needed

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(produce_fake_data())

