import asyncio
import random
import json
from dataclasses import asdict
from aiokafka import AIOKafkaProducer

from tests.fake_data.file_notification import fake_file_notification
from tests.fake_data.end_readout import fake_end_readout
from shared import constants

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"


async def produce_file_notifications():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        counter = 1
        while True:
            payload = fake_file_notification()
            msg = json.dumps(asdict(payload)).encode("utf-8")
            await producer.send_and_wait(constants.FILE_NOTIFICATION_TOPIC_NAME, msg)
            counter += 1
            await asyncio.sleep(random.uniform(1, 4))
    finally:
        await producer.stop()


async def produce_end_readouts():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        counter = 1
        while True:
            payload = fake_end_readout()
            msg = json.dumps(asdict(payload)).encode("utf-8")
            await producer.send_and_wait(constants.END_READOUT_TOPIC_NAME, msg)
            counter += 1
            await asyncio.sleep(random.uniform(2, 6))
    finally:
        await producer.stop()
# --- Main entry point ---


async def main():
    await asyncio.gather(
        produce_file_notifications(),
        produce_end_readouts(),
    )

if __name__ == "__main__":
    asyncio.run(main())

