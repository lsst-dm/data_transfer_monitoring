import os
from dataclasses import dataclass, field
from typing import Dict
from dataclasses_json import dataclass_json, config
from astropy.time import Time
import astropy.units as u

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
    additional_keys: str
    additional_values: str
    images_in_sequence: int
    image_name: str
    image_index: int
    image_source: str
    image_controller: str
    image_date: str
    image_number: int
    timestamp_acquisition_start: float
    requested_exposure_time: float
    timestamp_end_of_readout: float

    PENDING = "pending"
    COMPLETE = "complete"

    @classmethod
    def from_raw_message(cls, message):

        return EndReadoutModel(
            private_sndStamp=float(message["private_sndStamp"]),
            private_rcvStamp=float(message["private_rcvStamp"]),
            private_efdStamp=message["private_efdStamp"],
            private_kafkaStamp=message["private_kafkaStamp"],
            private_seqNum=message["private_seqNum"],
            private_revCode=message["private_revCode"],
            private_identity=message["private_identity"],
            private_origin=message["private_origin"],
            additional_keys=str(message["additional_keys"]),
            additional_values=str(message["additional_values"]),
            images_in_sequence=int(message["imagesInSequence"]),
            image_name=message["imageName"],
            image_index=int(message["imageIndex"]),
            image_source=message["imageSource"],
            image_controller=message["imageController"],
            image_date=str(message["imageDate"]),
            image_number=int(message["imageNumber"]),
            timestamp_acquisition_start=float(message["timestampAcquisitionStart"]),
            requested_exposure_time=float(message["requestedExposureTime"]),
            timestamp_end_of_readout=float(message["timestampEndOfReadout"])
        )

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
