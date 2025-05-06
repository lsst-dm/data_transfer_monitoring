from dataclasses import dataclass
from typing import Any, Dict


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
    additionalKeys: str
    additionalValues: str
    imagesInSequence: int
    imageName: str
    imageIndex: int
    imageSource: str
    imageController: str
    imageDate: str
    imageNumber: int
    timestampAcquisitionStart: float
    requestedExposureTime: float
    timestampEndOfReadout: float

    @classmethod
    def from_raw(cls, raw: Dict[str, Any]) -> "EndReadoutModel":
        return cls(**raw)

    @property
    def additional_fields(self) -> Dict[str, str]:
        """Returns a dict mapping additionalKeys to additionalValues."""
        keys = self.additionalKeys.split(":")
        values = self.additionalValues.split(":")
        return dict(zip(keys, values))
