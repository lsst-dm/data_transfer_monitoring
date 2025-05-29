import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

log = logging.getLogger(__name__)
log.info("Logging configured")
