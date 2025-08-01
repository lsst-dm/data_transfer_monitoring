from faker import Faker
from astropy.time import Time
import astropy.units as u
from datetime import datetime, timedelta
import random
import logging

from models.end_readout import EndReadoutModel

fake = Faker()
log = logging.getLogger(__name__)

def fake_tai_time(img_datetime):

    # Calculate start time (7 seconds ago in UTC)
    start_time_utc = img_datetime - timedelta(seconds=8)

    # Generate random UTC time within last 5 seconds
    random_seconds = random.uniform(0, 5)
    fake_utc = start_time_utc + timedelta(seconds=random_seconds)

    tai_time = Time(fake_utc).tai
    # Convert to TAI by adding fixed offset (37 seconds as of 2025)
    # tai_offset = timedelta(seconds=37)
    # log.info(f"fake tai time: {fake_utc + tai_offset}")
    # Assume t_tai is your Astropy Time object in TAI scale
    tai_epoch = Time('1958-01-01T00:00:00', scale='tai')

    # Calculate seconds since TAI epoch as a float
    seconds_since_epoch = (tai_time - tai_epoch).to(u.s).value

    # Format in scientific notation (e.g., with default or customized precision)
    sci_notation_str = f"{seconds_since_epoch:.15E}"  # 15 decimal places
    return float(sci_notation_str)

def fake_end_readout(img_obj) -> EndReadoutModel:
    img_datetime = img_obj.pop("image_datetime")
    return EndReadoutModel(
        private_sndStamp=fake.unix_time(),
        private_rcvStamp=0.0,
        private_efdStamp=fake.unix_time(),
        private_kafkaStamp=fake.unix_time(),
        private_seqNum=random.randint(1000000, 2000000),
        private_revCode=fake.hexify(text="^^^^^^^^"),
        private_identity=fake.user_name(),
        private_origin=random.randint(100000000, 999999999),
        additional_keys="imageType:groupId:testType:stutterRows:stutterNShifts:stutterDelay:reason:program",
        additional_values="BIAS:{}:BIAS:0:0:0.0:{}:{}".format(
            fake.iso8601(), fake.word().upper(), fake.word().upper()
        ),
        images_in_sequence=1,
        timestamp_acquisition_start=fake.unix_time(),
        requested_exposure_time=0.0,
        timestamp_end_of_readout=fake_tai_time(img_datetime),
        **img_obj
    )
