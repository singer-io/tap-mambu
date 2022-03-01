from unittest.mock import MagicMock

from ..constants import config_json


def test_centres_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.centres_generator import CentresGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = CentresGenerator(stream_name="centres",
                                 client=client_mock,
                                 config=config_json,
                                 state={'currently_syncing': 'centres'},
                                 sub_type="self")

    assert generator.endpoint_path == 'centres'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
            "sortBy": "lastModifiedDate:ASC",
            "detailsLevel": "FULL",
            "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
