from mambu_tests.helpers import MultithreadedBookmarkDayByDayGeneratorFake


def test_set_intermediary_bookmark():
    bookmark_field_name = 'testField'
    datetime_record = {'test_field': '2020-01-01T00:00:00.000000Z',
                       'dummy': 'data',
                       'number': 23}
    date_record = {'test_field': '2019-05-11',
                   'dummy': 'data2',
                   'number': 1}

    generator = MultithreadedBookmarkDayByDayGeneratorFake()
    generator.endpoint_bookmark_field = bookmark_field_name

    # test if the initial values aren't modified
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # test if the method exits if the bookmark isn't found in the record
    # also check if there isn't any variables set by mistake
    no_bookmark_field_record = {'test': '2020-01-01T00:00:00.000Z'}
    assert generator.set_intermediary_bookmark(no_bookmark_field_record) is None
    assert generator.endpoint_intermediary_bookmark_value is None
    assert generator.endpoint_intermediary_bookmark_offset == 0

    # check if the endpoint intermediary bookmark value and offset are set for a valid record with date field
    generator.set_intermediary_bookmark(date_record)
    assert generator.endpoint_intermediary_bookmark_value == date_record['test_field']
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset are set for a valid record with date time field
    generator.set_intermediary_bookmark(datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == datetime_record['test_field'][:10]
    assert generator.endpoint_intermediary_bookmark_offset == 1

    # check if the endpoint intermediary bookmark value and offset correctly set when multiple records have
    # the same bookmark field value
    for _ in range(3):
        generator.set_intermediary_bookmark(datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == datetime_record['test_field'][:10]
    assert generator.endpoint_intermediary_bookmark_offset == 4

    # check if the endpoint intermediary bookmark value is properly set and the offset is reset
    datetime_record['test_field'] = '2021-11-11T00:00:00.000000Z'
    generator.set_intermediary_bookmark(datetime_record)
    assert generator.endpoint_intermediary_bookmark_value == datetime_record['test_field'][:10]
    assert generator.endpoint_intermediary_bookmark_offset == 1
