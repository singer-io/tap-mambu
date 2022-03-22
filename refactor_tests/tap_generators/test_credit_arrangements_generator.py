from unittest.mock import MagicMock

from ..constants import config_json


def test_credit_arrangements_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.credit_arrangements_generator import CreditArrangementsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = CreditArrangementsGenerator(stream_name="credit_arrangements",
                                            client=client_mock,
                                            config=config_json,
                                            state={'currently_syncing': 'credit_arrangements'},
                                            sub_type="self")

    assert generator.endpoint_path == 'creditarrangements'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
        "sortBy": "creationDate:ASC",
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
