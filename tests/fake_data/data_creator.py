from faker import Faker
import random
from collections import OrderedDict

from tests.fake_data.end_readout import fake_end_readout
from tests.fake_data.file_notification import fake_file_notification
from tests.fake_data.expected_sensors import fake_expected_sensors


class DataCreator(object):
    expected_missing_files = 0
    fake = Faker()
    def random_image_object(self):
        # Randomly pick source and controller codes
        image_source = self.fake.random_element(
            elements=OrderedDict([("MC", 0.9), ("SC", 0.05), ("XC", 0.05)])
        )
        image_controller = self.fake.random_element(elements=["O", "A", "B"])
        # Generate a recent date and format as YYYYMMDD
        date_obj = self.fake.date_between(start_date="-30d", end_date="today")
        image_date = date_obj.strftime("%Y%m%d")
        # Random image number and index
        image_number = random.randint(1, 99999)
        image_index = random.randint(1, 10)
        # Compose imageName
        image_name = f"{image_source}_{image_controller}_{image_date}_{image_number:06d}"
        return {
            "image_name": image_name,
            "image_index": image_index,
            "image_source": image_source,
            "image_controller": image_controller,
            "image_date": image_date,
            "image_number": image_number,
        }

    def weighted_random_float(self):
        # random.random() gives a float in [0, 1)
        # Squaring it biases towards 0
        return (random.random() ** 2) * 0.0005

    def create_fake_data(self):
        file_failure_rate = self.weighted_random_float()
        img_obj = self.random_image_object()
        expected_sensors = fake_expected_sensors(img_obj)
        sensor_names = expected_sensors.expected_sensors.keys()

        json_file_objects = []
        for sensor_name in sensor_names:
            rand_num = random.random()
            should_fail_write = rand_num < file_failure_rate
            if should_fail_write:
                print("failing to write file")
                self.expected_missing_files += 1
                print(f"expected missing files: {self.expected_missing_files}")
                continue
            else:
                json_file_objects.append(
                    fake_file_notification(img_obj, sensor_name, ".json")
                )

        fits_file_objects = []
        for sensor_name in sensor_names:
            should_fail_write = random.random() < file_failure_rate
            if should_fail_write:
                continue
            else:
                fits_file_objects.append(
                    fake_file_notification(img_obj, sensor_name, ".fits")
                )

        all_file_objects = json_file_objects + fits_file_objects
        end_readout = fake_end_readout(img_obj)

        return expected_sensors, all_file_objects, end_readout
