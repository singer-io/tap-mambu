from unittest.mock import MagicMock

from ..constants import config_json


def test_interest_accrual_breakdown_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.interest_accrual_breakdown_generator import \
        InterestAccrualBreakdownGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = InterestAccrualBreakdownGenerator(stream_name="interest_accrual_breakdown",
                                                  client=client_mock,
                                                  config=config_json,
                                                  state={'currently_syncing': 'interest_accrual_breakdown'},
                                                  sub_type="self")

    assert generator.endpoint_path == 'accounting/interestaccrual:search'
    assert generator.endpoint_bookmark_field == "creationDate"
    assert generator.endpoint_sorting_criteria == {
        "field": "creationDate",
        "order": "ASC"
    }
    assert generator.endpoint_filter_criteria == [
        {
            "field": "creationDate",
            "operator": "AFTER",
            "value": '2021-06-01'
        }
    ]
