from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RealtimeStatus(_message.Message):
    __slots__ = ("picked_count", "shipped_count", "pod_count", "completion_rate", "avg_delivery_time")
    PICKED_COUNT_FIELD_NUMBER: _ClassVar[int]
    SHIPPED_COUNT_FIELD_NUMBER: _ClassVar[int]
    POD_COUNT_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_RATE_FIELD_NUMBER: _ClassVar[int]
    AVG_DELIVERY_TIME_FIELD_NUMBER: _ClassVar[int]
    picked_count: int
    shipped_count: int
    pod_count: int
    completion_rate: float
    avg_delivery_time: float
    def __init__(self, picked_count: _Optional[int] = ..., shipped_count: _Optional[int] = ..., pod_count: _Optional[int] = ..., completion_rate: _Optional[float] = ..., avg_delivery_time: _Optional[float] = ...) -> None: ...
