from mambu_tests.helpers import MultithreadedBookmarkDayByDayGeneratorFake
from tap_mambu.helpers.datetime_utils import datetime_to_tz, str_to_localized_datetime


def test_set_intermediary_bookmark_datetime():
    mock_datetime_record = datetime_to_tz(str_to_localized_datetime('2021-01-01T00:00:00.000000Z'), "UTC")

    generator = MultithreadedBookmarkDayByDayGeneratorFake()

    # test if the initial values aren't modified
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # check if the endpoint intermediary bookmark value and offset are set for a valid record with date time field
    generator.set_intermediary_bookmark(mock_datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_datetime_record.replace(hour=0, minute=0,
                                                                                          second=0, microsecond=0)
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value isn't set with a value lower than the already saved one
    generator.set_intermediary_bookmark(datetime_to_tz(str_to_localized_datetime('2005-05-14T00:00:00.000000Z'), "UTC"))
    assert generator.endpoint_intermediary_bookmark_value == mock_datetime_record.replace(hour=0, minute=0,
                                                                                          second=0, microsecond=0)
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset correctly set when multiple records have
    # the same bookmark field value
    for _ in range(3):
        generator.set_intermediary_bookmark(mock_datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_datetime_record.replace(hour=0, minute=0,
                                                                                          second=0, microsecond=0)
    assert generator.endpoint_intermediary_bookmark_offset == 4

    # check if the endpoint intermediary bookmark value is properly set and the offset is reset
    mock_datetime_record = datetime_to_tz(str_to_localized_datetime('2021-11-11T00:00:00.000000Z'), "UTC")
    generator.set_intermediary_bookmark(mock_datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_datetime_record.replace(hour=0, minute=0,
                                                                                          second=0, microsecond=0)
    assert generator.endpoint_intermediary_bookmark_offset == 1


def test_set_intermediary_bookmark_date():
    mock_date_record = datetime_to_tz(str_to_localized_datetime('2020-01-01'), "UTC")

    generator = MultithreadedBookmarkDayByDayGeneratorFake()

    # test if the initial values aren't modified
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # check if the endpoint intermediary bookmark value and offset are set for a valid record with date field
    generator.set_intermediary_bookmark(mock_date_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_date_record
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value isn't set with a value lower than the already saved one
    generator.set_intermediary_bookmark(datetime_to_tz(str_to_localized_datetime('2005-05-14'), "UTC"))
    assert generator.endpoint_intermediary_bookmark_value == mock_date_record
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset correctly set when multiple records have
    # the same bookmark field value
    for _ in range(3):
        generator.set_intermediary_bookmark(mock_date_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_date_record
    assert generator.endpoint_intermediary_bookmark_offset == 4

    # check if the endpoint intermediary bookmark value is properly set and the offset is reset
    mock_date_record = datetime_to_tz(str_to_localized_datetime('2021-11-11'), "UTC")
    generator.set_intermediary_bookmark(mock_date_record)
    assert generator.endpoint_intermediary_bookmark_value == mock_date_record
    assert generator.endpoint_intermediary_bookmark_offset == 1
