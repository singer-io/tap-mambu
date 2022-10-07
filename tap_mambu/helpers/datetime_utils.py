from pytz import timezone
from datetime import datetime
from tap_mambu import MambuClient
import dateutil.parser


_timezone = None
_datetime_formats = [
    "%Y-%m-%dT%H:%M:%S.%fZ%z",  # 0000-00-00T00:00:00.000000Z+00:00
    "%Y-%m-%dT%H:%M:%S.%f%z",   # 0000-00-00T00:00:00.000000+00:00
    "%Y-%m-%dT%H:%M:%SZ%z",     # 0000-00-00T00:00:00Z+00:00
    "%Y-%m-%dT%H:%M:%S%z",      # 0000-00-00T00:00:00+00:00
    "%Y-%m-%dT%H:%M%z",         # 0000-00-00T00:00+00:00

    "%Y-%m-%dT%H:%M:%S.%fZ",    # 0000-00-00T00:00:00.000000Z
    "%Y-%m-%dT%H:%M:%S.%f",     # 0000-00-00T00:00:00.000000
    "%Y-%m-%dT%H:%M:%SZ",       # 0000-00-00T00:00:00Z
    "%Y-%m-%dT%H:%M:%S",        # 0000-00-00T00:00:00
    "%Y-%m-%dT%H:%M",           # 0000-00-00T00:00

    "%Y-%m-%d",                 # 0000-00-00
]


def get_timezone_info(client: MambuClient):
    global _timezone
    response = client.request(method="GET", path="settings/organization", version="v1")
    _timezone = timezone(response.get("timeZoneID"))


def localize(dttm: datetime) -> datetime:
    if _timezone is None:
        raise RuntimeError("Cannot use timezone information without first calling 'get_timezone_info'")
    if dttm.tzinfo is None:  # If no timezone information is provided, we assume the datetime is in UTC format
        dttm = timezone("UTC").localize(dttm)
    # Convert datetime to Tenant Timezone
    return dttm.astimezone(_timezone)


def str_to_datetime(dttm_str: str) -> datetime:
    for datetime_format in _datetime_formats:
        try:
            return datetime.strptime(dttm_str, datetime_format)
        except ValueError:
            pass
    raise ValueError(f"Failed parsing datetime from string ({dttm_str})")


def str_to_localized_datetime(dttm_str: str) -> datetime:
    return localize(str_to_datetime(dttm_str))


def datetime_to_utc_str(dttm: datetime) -> str:
    return dttm.astimezone(timezone("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def datetime_to_local_str(dttm: datetime) -> str:
    return dttm.astimezone(_timezone).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def datetime_to_tz(dttm: datetime, tz: str) -> datetime:
    return dttm.astimezone(timezone(tz))


def utc_now():
    return datetime.now(timezone("UTC"))


def local_now():
    return datetime.now(_timezone)
