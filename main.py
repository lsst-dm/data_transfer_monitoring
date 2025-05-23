import asyncio
from prometheus_client import start_http_server

from shared import config
from shared import constants
from listeners.file_notifications import FileNotificationListener
from listeners.end_readout import EndReadoutListener
from local_producers import produce_fake_data
# from local_producers import produce_file_notifications, produce_end_readouts

# file notification with expected sensors name in it
# expected sensors lives in s3 bucket

# topics are file notifications and end readout

# counter for .fits file
# counter for .json file
# counter for end run kafka message
# if files are missing then generate a log message
# coutner for missing files over time
#
# add histogram summary over sliding time window
# add summary of files processed during the window: prometheus Summary
# all metrics are valid over the observation window, files come in every 30 seconds
#
# log error if a file comes in late or is missing
# TODO image source is MC, if its not MC then log it and pass on the end readout event
#
#
#
# TODO not sure we need persistence, we could just query aws for the actual images.
# this would work even for failover


async def main():
    tasks = []
    if config.IS_PROD == "False":
        # start local producers if we're on local
        tasks.append(produce_fake_data())
    # start prometheus
    start_http_server(8000)

    # start our kafka listeners
    listeners = [
        FileNotificationListener(constants.FILE_NOTIFICATION_TOPIC_NAME).start(),
        EndReadoutListener(constants.END_READOUT_TOPIC_NAME).start()
    ]

    tasks.extend(listeners)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

