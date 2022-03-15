from unittest.mock import MagicMock

from ..constants import config_json


def test_communications_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.communications_generator import CommunicationsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = CommunicationsGenerator(stream_name="communications",
                                        client=client_mock,
                                        config=config_json,
                                        state={'currently_syncing': 'communications'},
                                        sub_type="self")

    assert generator.endpoint_path == 'communications/messages:search'
    assert generator.endpoint_params == {
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "creationDate"
    assert not hasattr(generator.endpoint_body, 'filterCriteria')
    assert not hasattr(generator.endpoint_body, 'sortingCriteria')
