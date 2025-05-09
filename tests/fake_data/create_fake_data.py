from faker import Faker
import random

from tests.fake_data.end_readout import fake_end_readout
from tests.fake_data.file_notification import fake_file_notification
from tests.fake_data.expected_sensors import fake_expected_sensors

fake = Faker()


def random_image_object():
    # Randomly pick source and controller codes
    image_source = fake.random_element(elements=["MC", "SC", "XC"])
    image_controller = fake.random_element(elements=["O", "A", "B"])
    # Generate a recent date and format as YYYYMMDD
    date_obj = fake.date_between(start_date="-30d", end_date="today")
    image_date = date_obj.strftime("%Y%m%d")
    # Random image number and index
    image_number = random.randint(1, 99999)
    image_index = random.randint(1, 10)
    # Compose imageName
    image_name = f"{image_source}_{image_controller}_{image_date}_{image_number:06d}"
    return {
        "imageName": image_name,
        "imageIndex": image_index,
        "imageSource": image_source,
        "imageController": image_controller,
        "imageDate": image_date,
        "imageNumber": image_number,
    }


def weighted_random_float():
    # random.random() gives a float in [0, 1)
    # Squaring it biases towards 0
    return (random.random() ** 2) * 0.1


def create_fake_data():
    file_failure_rate = weighted_random_float()
    img_obj = random_image_object()
    expected_sensors = fake_expected_sensors(img_obj)
    sensor_names = expected_sensors.expected_sensors.keys()
    json_file_objects = [
        fake_file_notification(img_obj, sensor_name, ".json")
        for sensor_name in sensor_names
        if random.random() > file_failure_rate
    ]
    fits_file_objects = [
        fake_file_notification(img_obj, sensor_name, ".fits")
        for sensor_name in sensor_names
        if random.random() > file_failure_rate
    ]
    all_file_objects = json_file_objects + fits_file_objects
    end_readout = fake_end_readout(img_obj)

    return expected_sensors, all_file_objects, end_readout
