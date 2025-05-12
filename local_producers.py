import asyncio
import random
import json
from dataclasses import asdict
from aiokafka import AIOKafkaProducer

from tests.fake_data.create_fake_data import create_fake_data

from shared import constants
from shared.aws_client import AsyncS3Client

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"


# async def produce_file_notifications():
#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     await producer.start()
#     try:
#         counter = 1
#         while True:
#             payload = fake_file_notification()
#             msg = json.dumps(asdict(payload)).encode("utf-8")
#             await producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)
#             counter += 1
#             await asyncio.sleep(random.uniform(1, 20))
#     finally:
#         await producer.stop()


# async def produce_end_readouts():
#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     await producer.start()
#     try:
#         counter = 1
#         while True:
#             payload = fake_end_readout()
#             msg = json.dumps(asdict(payload)).encode("utf-8")
#             await producer.send_and_wait(constants.END_READOUT_TOPIC_NAME, msg)
#             counter += 1
#             await asyncio.sleep(random.uniform(2, 6))
#     finally:
#         await producer.stop()


# async def produce_fake_data():
#     storage_client = AsyncS3Client()
#     await storage_client.initialize()
#     producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
#     await producer.start

#     while True:
#         expected_sensors, all_files, end_readout = create_fake_data()
#         for file_obj in all_files:
#             # might need to check that the date doesnt already exist in the storage bucket when generating
#             key = file_obj.records[0].s3.object.key
#             await storage_client.upload_file(key)

#         await storage_client.upload_file(expected_sensors.storage_key, json_body=expected_sensors.to_json())

async def produce_fake_data():
    storage_client = AsyncS3Client()
    await storage_client.initialize()
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
            print("uploading files")
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
                print(f"Late file notification sent for: {key}")

            await asyncio.sleep(random.randint(5, 7))  # Adjust as needed

    finally:
        await producer.stop()



if __name__ == "__main__":
    asyncio.run(produce_fake_data())

