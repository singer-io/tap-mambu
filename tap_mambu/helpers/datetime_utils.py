from singer.transform import string_to_datetime, strptime_to_utc
from pytz import timezone
from datetime import datetime
from tap_mambu import MambuClient
import dateutil.parser


_timezone = None


def get_timezone_info(client: MambuClient):
    global _timezone
    response = client.request(method="GET", path="setup/organization", version="v2")
    _timezone = timezone(response.get("timeZoneID"))


def localize(dttm: datetime) -> datetime:
    if _timezone is None:
        raise RuntimeError("Cannot use timezone information without first calling 'get_timezone_info'")
    if dttm.tzinfo is None:  # If no timezone information is provided, we assume the datetime is in UTC format
        dttm = timezone("UTC").localize(dttm)
    # Convert datetime to Tenant Timezone
    return dttm.astimezone(_timezone)


def str_to_datetime(dttm_str: str) -> datetime:
    return dateutil.parser.parse(dttm_str)


def str_to_localized_datetime(dttm_str: str) -> datetime:
    return localize(str_to_datetime(dttm_str))


def datetime_to_utc_str(dttm: datetime) -> str:
    return dttm.astimezone(timezone("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
