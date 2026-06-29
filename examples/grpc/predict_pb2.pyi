from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PredictRequest(_message.Message):
    __slots__ = ("features",)
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    features: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, features: _Optional[_Iterable[float]] = ...) -> None: ...

class PredictResponse(_message.Message):
    __slots__ = ("prediction", "error")
    PREDICTION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    prediction: float
    error: str
    def __init__(self, prediction: _Optional[float] = ..., error: _Optional[str] = ...) -> None: ...
