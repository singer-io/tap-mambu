import mock
from mock import MagicMock
from copy import deepcopy
from . import setup_generator_base_test
from ..constants import config_json


@mock.patch("tap_mambu.tap_generators.audit_trail_generator.utils")
def test_audit_trail_generator_endpoint_config_init(utils_mock):
    fake_lte_date = "2022-01-01T00:00:00.000000Z"
    utils_mock.strftime.return_value = fake_lte_date

    generators = setup_generator_base_test("audit_trail")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'v1/events'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_api_version == "v1"
    assert generator.endpoint_api_key_type == "audit"
    assert generator.endpoint_params == {
        "sort_order": "asc",
        "occurred_at[gte]": "2021-06-01T00:00:00.000000Z",
        "occurred_at[lte]": fake_lte_date
    }
    assert generator.endpoint_bookmark_field == "occurred_at"
    assert generator.audit_trail_offset == 0


@mock.patch("tap_mambu.tap_generators.audit_trail_generator.get_bookmark")
@mock.patch("tap_mambu.tap_generators.audit_trail_generator.utils")
def test_audit_trail_generator_bookmark(utils_mock, get_bookmark_mock):
    fake_gte_date = "2021-09-01T00:00:00.000000Z"
    fake_lte_date = "2022-01-01T00:00:00.000000Z"
    get_bookmark_mock.return_value = [fake_gte_date, 2]
    utils_mock.strftime.return_value = fake_lte_date

    generators = setup_generator_base_test("audit_trail")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.audit_trail_offset == 2
    assert generator.endpoint_params == {
        "sort_order": "asc",
        "occurred_at[gte]": fake_gte_date,
        "occurred_at[lte]": fake_lte_date
    }


def test_generator_iterator_flow():
    client_mock = MagicMock()
    client_mock.page_size = 5
    client_data = [[
        {"id": nr + page*5, "occurred_at": f"2021-10-01T00:00:{nr+page*5:02d}.000000Z"}
        for nr in range(5)] for page in range(3)]
    from tap_mambu.tap_generators.audit_trail_generator import AuditTrailGenerator
    generator = AuditTrailGenerator(stream_name="audit_trail",
                                    client=client_mock,
                                    config=config_json,
                                    state={"currently_syncing": "audit_trail"},
                                    sub_type="self")
    generator.prepare_batch = MagicMock()
    generator.fetch_batch = MagicMock(side_effect=client_data)

    expected_records = [
        {"custom_fields": [], "id": record["id"], "occurred_at": record["occurred_at"]}
        for record in deepcopy(sum(client_data, []))]
    record_count = 0
    for record in generator:
        assert record == expected_records[record_count]
        record_count += 1
