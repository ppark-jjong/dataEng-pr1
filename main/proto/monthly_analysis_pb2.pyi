from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MonthlyAnalysis(_message.Message):
    __slots__ = ("weekly_completion_rate", "issue_pattern", "sla_counts")
    class IssuePatternEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class SlaCountsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    WEEKLY_COMPLETION_RATE_FIELD_NUMBER: _ClassVar[int]
    ISSUE_PATTERN_FIELD_NUMBER: _ClassVar[int]
    SLA_COUNTS_FIELD_NUMBER: _ClassVar[int]
    weekly_completion_rate: float
    issue_pattern: _containers.ScalarMap[str, int]
    sla_counts: _containers.ScalarMap[str, int]
    def __init__(self, weekly_completion_rate: _Optional[float] = ..., issue_pattern: _Optional[_Mapping[str, int]] = ..., sla_counts: _Optional[_Mapping[str, int]] = ...) -> None: ...
