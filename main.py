import asyncio
import logging
import sys
from prometheus_client import start_http_server

from shared import constants
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
# Logging config
if constants.DEBUG_LOGS == "true":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
else:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

log = logging.getLogger(__name__)


async def main():
    tasks = []

    # start prometheus
    start_http_server(8000)

    # start our kafka listeners
    log.info("starting file notification listener...")
    file_notification_params = {
        "topic": constants.FILE_NOTIFICATION_TOPIC_NAME,
        "bootstrap_servers": constants.FILE_NOTIFICATION_KAFKA_BOOTSTRAP_SERVERS,
        "group_id": constants.FILE_NOTIFICATION_KAFKA_GROUP_ID,
    }
    tasks.append(
        FileNotificationListener(
            **file_notification_params
        ).start()
    )

    if constants.SHOULD_RUN_END_READOUT_LISTENER:
        log.info("starting end readout listener")
        end_readout_listener_params = {
            "topic": constants.END_READOUT_TOPIC_NAME,
            "bootstrap_servers": constants.END_READOUT_KAFKA_BOOTSTRAP_SERVERS,
            "group_id": constants.END_READOUT_KAFKA_GROUP_ID,
        }
        if constants.IS_PROD == "True":
            end_readout_listener_params["schema_registry"] = constants.END_READOUT_SCHEMA_REGISTRY
            end_readout_listener_params["auth"] = {
                    "security_proticol": constants.END_READOUT_SECURITY_PROTOCOL,
                    "sasl_mechanism": constants.END_READOUT_SASL_MECHANISM,
                    "sasl_plain_username": constants.END_READOUT_SASL_USERNAME,
                    "sasl_plain_password": constants.END_READOUT_SASL_PASSWORD,
                }
        tasks.append(
            EndReadoutListener(
                **end_readout_listener_params
            ).start()
        )

    log.info("starting notification tracker periodic cleanup task...")
    await NotificationTracker.start_periodic_cleanup(
        interval_seconds=constants.NOTIFICATION_CLEANUP_INTERVAL
    )
    log.info("started periodic cleanup successfully")

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
