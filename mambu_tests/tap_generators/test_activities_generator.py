import mock
from singer import utils
from . import setup_generator_base_test


def test_activities_generator_endpoint_config_init():
    generators = setup_generator_base_test("activities")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'activities'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_api_version == "v1"
    assert generator.endpoint_params == {
        "from": "2021-06-01",
        "to": utils.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:10],
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "timestamp"


def test_activities_generator_dict_unpacking():
    generators = setup_generator_base_test("activities", with_data=True,
                                           custom_data=[{
                                               "client": "N/A", "activity": {"id": index}}
                                               for index in range(1000)])

    assert 1 == len(generators)

    generator = generators[0]

    for record in generator:
        assert "id" in record, "Record in activity generator were not unpacked " \
                               "according to spec! (see fetch_batch)"
        assert "client" in record, "Record in activity generator were not unpacked " \
                                   "according to spec! (see fetch_batch)"
        assert "activity" not in record, "Record in activity generator were not unpacked " \
                                         "according to spec! (see fetch_batch)"
