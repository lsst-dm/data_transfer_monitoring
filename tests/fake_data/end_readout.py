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

    # Convert to TAI time
    tai_time = Time(fake_utc).tai

    # Unix epoch in TAI scale (1970-01-01 00:00:00 TAI)
    unix_epoch_tai = Time('1970-01-01T00:00:00', scale='tai')

    # Calculate Unix TAI time (seconds since 1970-01-01 00:00:00 TAI)
    # This is different from regular Unix time which is UTC-based
    unix_tai_seconds = (tai_time - unix_epoch_tai).to(u.s).value

    return unix_tai_seconds

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
