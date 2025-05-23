import asyncio
import logging
from prometheus_client import start_http_server

from shared import constants
from shared import config
from listeners.file_notifications import FileNotificationListener
from listeners.end_readout import EndReadoutListener
from shared.notifications.notification_tracker import NotificationTracker

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

# images get generated every 7 seconds
# end readout is in tai time

# timestamp end of readout compare to when files arrive to make sure they arrive within the window (7 seconds)
# file notifications should be sent out within 7 seconds of the timestampEndOfReadout time (tai time)
# file notification times are UTC

# unexplained file omission (UFO)


async def main():
    tasks = []

    # start prometheus
    start_http_server(8000)

    # start our kafka listeners
    tasks.append(
        FileNotificationListener(constants.FILE_NOTIFICATION_TOPIC_NAME).start()
    )
    if config.SHOULD_RUN_END_READOUT_LISTENER:
        logging.info("starting end readout listener")
        tasks.append(EndReadoutListener(constants.END_READOUT_TOPIC_NAME).start())

    await NotificationTracker.start_periodic_cleanup(
        interval_seconds=constants.NOTIFICATION_CLEANUP_INTERVAL
    )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
