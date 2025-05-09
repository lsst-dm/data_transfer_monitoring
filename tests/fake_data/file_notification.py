from faker import Faker
import random

from models.file_notification import (
    FileNotificationModel,
    OwnerIdentity,
    S3Bucket,
    S3Object,
    S3Info,
    UserIdentity,
    RequestParameters,
    ResponseElements,
    Record,
)

fake = Faker()


def fake_owner_identity():
    return OwnerIdentity(principal_id=fake.user_name())


def fake_s3_bucket():
    return S3Bucket(
        name=fake.domain_word(),
        owner_identity=fake_owner_identity(),
        arn=f"arn:aws:s3:{fake.word()}::{fake.domain_word()}",
        id=fake.uuid4(),
    )


def fake_s3_object(img_obj, sensor_name, file_extension):
    image_date = img_obj["imageDate"]
    observation_id = img_obj["imageName"]
    filename = f"{observation_id}_{sensor_name}{file_extension}"
    # "key": "LSSTCam/20250423/MC_O_20250423_000034/MC_O_20250423_000034_R31_S10.json",
    return S3Object(
        key=f"LSSTCam/{image_date}/{observation_id}/{filename}",
        size=random.randint(100, 10000),
        e_tag=fake.md5(),
        version_id="",
        sequencer=fake.sha1()[:16].upper(),
        metadata=[],
        tags=[],
    )


def fake_s3_info(img_obj, sensor_name, file_extension):
    return S3Info(
        s3_schema_version="1.0",
        configuration_id=fake.word(),
        bucket=fake_s3_bucket(),
        object=fake_s3_object(img_obj, sensor_name, file_extension),
    )


def fake_user_identity():
    return UserIdentity(principal_id=fake.user_name())


def fake_request_parameters():
    return RequestParameters(source_ip_address=fake.ipv4())


def fake_response_elements():
    return ResponseElements(x_amz_request_id=fake.uuid4(), x_amz_id_2=fake.uuid4())


def fake_record(img_obj, sensor_name, file_extension):
    return Record(
        event_version="2.2",
        event_source="ceph:s3",
        aws_region=fake.word(),
        event_time=str(fake.date_time_this_year()),
        event_name="ObjectCreated:Put",
        user_identity=fake_user_identity(),
        request_parameters=fake_request_parameters(),
        response_elements=fake_response_elements(),
        s3=fake_s3_info(img_obj, sensor_name, file_extension),
        event_id=f"{random.randint(1000000000, 9999999999)}.{random.randint(100000,999999)}.{fake.md5()}",
        opaque_data="",
    )


def fake_file_notification(img_obj, sensor_name, file_extension, num_records=1):

    return FileNotificationModel(
        records=[
            fake_record(img_obj, sensor_name, file_extension)
            for _ in range(num_records)
        ]
    )
