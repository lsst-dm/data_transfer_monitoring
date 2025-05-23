from faker import Faker
from datetime import datetime, timedelta
import random

from models.end_readout import EndReadoutModel

fake = Faker()

def fake_tai_time():
    # Get current UTC time
    now_utc = datetime.utcnow()
    
    # Calculate start time (7 seconds ago in UTC)
    start_time_utc = now_utc - timedelta(seconds=8)
    
    # Generate random UTC time within last 5 seconds
    random_seconds = random.uniform(0, 5)
    fake_utc = start_time_utc + timedelta(seconds=random_seconds)
    
    # Convert to TAI by adding fixed offset (37 seconds as of 2025)
    tai_offset = timedelta(seconds=37)
    return fake_utc + tai_offset

def fake_end_readout(img_obj) -> EndReadoutModel:
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
        timestamp_end_of_readout=fake_tai_time(),
        **img_obj
    )

