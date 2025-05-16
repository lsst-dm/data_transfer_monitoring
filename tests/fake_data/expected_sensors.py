from faker import Faker

from models.expected_sensors import ExpectedSensorsModel

fake = Faker()


def generate_expected_sensors():
    sensors = {}
    # Example: generate keys similar to your sample
    # R00_SW0, R00_SW1, R01_S00, ..., R44_SW1
    # For demonstration, we'll generate a subset; expand as needed
    for r in range(0, 45):
        if r % 10 == 0 or r % 10 == 4:  # For R00_SW0, R00_SW1, R04_SW0, etc.
            for sw in range(2):
                sensors[f"R{r:02d}_SW{sw}"] = fake.random_element(
                    elements=["SCIENCE", "ENGINEERING", "CALIBRATION"]
                )
        if r % 10 != 0 and r % 10 != 4:  # For R01_S00, R01_S01, ..., R43_S22
            for s1 in range(3):
                for s2 in range(3):
                    sensors[f"R{r:02d}_S{s1}{s2}"] = fake.random_element(
                        elements=["SCIENCE", "ENGINEERING", "CALIBRATION"]
                    )
    return sensors


def generate_expected_sensors_object(img_obj):
    obs_id = img_obj["image_name"]
    file_name = f"{obs_id}_expectedSensors.json"
    return {
        "fileName": file_name,
        "fileType": "expectedSensors",
        "obsId": obs_id,
        "version": 1.0,
        "expectedSensors": generate_expected_sensors(),
    }


def fake_expected_sensors(img_obj) -> ExpectedSensorsModel:
    obs_id = img_obj["image_name"]
    file_name = f"{obs_id}_expectedSensors.json"
    return ExpectedSensorsModel(
        file_name=file_name,
        file_type="expected_sensors",
        obs_id=obs_id,
        version=1.0,
        expected_sensors=generate_expected_sensors(),
    )
