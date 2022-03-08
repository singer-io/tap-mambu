from unittest.mock import MagicMock

from ..constants import config_json
from ..helpers import IsInstanceMatcher


def test_installments_generator_endpoint_config_init():
    from singer import utils
    from tap_mambu.tap_mambu_refactor.tap_generators.installments_generator import InstallmentsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = InstallmentsGenerator(stream_name="installments",
                                      client=client_mock,
                                      config=config_json,
                                      state={'currently_syncing': 'installments'},
                                      sub_type="self")

    assert generator.endpoint_path == 'installments'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
        "dueFrom": "2021-06-01",
        "dueTo": utils.now().strftime("%Y-%m-%d"),
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastPaidDate"
