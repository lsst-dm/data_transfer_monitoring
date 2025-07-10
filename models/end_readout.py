import os
from dataclasses import dataclass, field
from typing import Dict, Optional
from dataclasses_json import dataclass_json, config
from astropy.time import Time
import astropy.units as u
from models.expected_sensors import ExpectedSensorsModel
from shared import constants


@dataclass_json
@dataclass(frozen=True, kw_only=True)
class EndReadoutModel:
    private_sndStamp: float
    private_rcvStamp: float
    private_efdStamp: float
    private_kafkaStamp: float
    private_seqNum: int
    private_revCode: str
    private_identity: str
    private_origin: int
    additional_keys: str = field(metadata=config(field_name="additionalKeys"))
    additional_values: str = field(metadata=config(field_name="additionalValues"))
    images_in_sequence: int = field(metadata=config(field_name="imagesInSequence"))
    image_name: str = field(metadata=config(field_name="imageName"))
    image_index: int = field(metadata=config(field_name="imageIndex"))
    image_source: str = field(metadata=config(field_name="imageSource"))
    image_controller: str = field(metadata=config(field_name="imageController"))
    image_date: str = field(metadata=config(field_name="imageDate"))
    image_number: int = field(metadata=config(field_name="imageNumber"))
    timestamp_acquisition_start: float = field(metadata=config(field_name="timestampAcquisitionStart"))
    requested_exposure_time: float = field(metadata=config(field_name="requestedExposureTime"))
    timestamp_end_of_readout: float = field(metadata=config(field_name="timestampEndOfReadout"))

    PENDING = "pending"
    COMPLETE = "complete"

    @property
    def additional_fields(self) -> Dict[str, str]:
        """Returns a dict mapping additionalKeys to additionalValues."""
        keys = self.additional_keys.split(":")
        values = self.additional_values.split(":")
        return dict(zip(keys, values))

    @property
    def expected_sensors_folder_prefix(self):
        return os.path.join("LSSTCam", self.image_date, self.image_name)

    @property
    def id(self):
        expected_sensors_filename = f"{self.image_name}_{constants.EXPECTED_SENSORS_FILENAME}"
        return os.path.join(self.expected_sensors_folder_prefix, expected_sensors_filename)

    @property
    def timestamp(self):
        """
            Returns a python utc datetime
        """
        # Create a Time object at the TAI epoch
        tai_epoch = Time('1958-01-01T00:00:00', scale='tai')

        # Add the seconds to the epoch
        t = tai_epoch + self.timestamp_end_of_readout * u.s

        # Convert to UTC and extract Python datetime
        dt_utc = t.utc.datetime
        return dt_utc
