import asyncio

from tests.emulator import Emulator


async def main():
    emulator = Emulator()
    await emulator.initialize()
    await emulator.produce_fake_data()

if __name__ == "__main__":
    asyncio.run(main())

