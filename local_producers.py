import asyncio
import logging
import sys

from tests.emulator import Emulator
from shared import constants


if constants.DEBUG_LOGS == "true":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
else:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

log = logging.getLogger(__name__)

async def main():
    emulator = Emulator()
    await emulator.initialize()
    await emulator.produce_fake_data()

if __name__ == "__main__":
    asyncio.run(main())
