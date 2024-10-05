from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class WeeklyAnalysis(_message.Message):
    __slots__ = ("avg_distance", "issues_dps", "issue_count")
    AVG_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    ISSUES_DPS_FIELD_NUMBER: _ClassVar[int]
    ISSUE_COUNT_FIELD_NUMBER: _ClassVar[int]
    avg_distance: float
    issues_dps: _containers.RepeatedScalarFieldContainer[str]
    issue_count: int
    def __init__(self, avg_distance: _Optional[float] = ..., issues_dps: _Optional[_Iterable[str]] = ..., issue_count: _Optional[int] = ...) -> None: ...
