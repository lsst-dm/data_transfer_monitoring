from faker import Faker
import random

from models.end_readout import EndReadoutModel

fake = Faker()


def fake_end_readout() -> EndReadoutModel:
    return EndReadoutModel(
        private_sndStamp=fake.unix_time(),
        private_rcvStamp=0.0,
        private_efdStamp=fake.unix_time(),
        private_kafkaStamp=fake.unix_time(),
        private_seqNum=random.randint(1000000, 2000000),
        private_revCode=fake.hexify(text="^^^^^^^^"),
        private_identity=fake.user_name(),
        private_origin=random.randint(100000000, 999999999),
        additionalKeys="imageType:groupId:testType:stutterRows:stutterNShifts:stutterDelay:reason:program",
        additionalValues="BIAS:{}:BIAS:0:0:0.0:{}:{}".format(
            fake.iso8601(), fake.word().upper(), fake.word().upper()
        ),
        imagesInSequence=1,
        imageName=fake.bothify(text="MC_O_########_######"),
        imageIndex=1,
        imageSource=fake.random_element(elements=("MC", "SC", "EC")),
        imageController=fake.random_letter().upper(),
        imageDate=fake.date(pattern="%Y%m%d"),
        imageNumber=random.randint(1, 100),
        timestampAcquisitionStart=fake.unix_time(),
        requestedExposureTime=0.0,
        timestampEndOfReadout=fake.unix_time()
    )

