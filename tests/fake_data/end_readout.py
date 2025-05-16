from faker import Faker
import random

from models.end_readout import EndReadoutModel

fake = Faker()


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
        timestamp_end_of_readout=fake.unix_time(),
        **img_obj
    )

