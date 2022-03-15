from unittest.mock import MagicMock

from ..constants import config_json


def test_branches_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.index_rate_sources_generator import IndexRateSourcesGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = IndexRateSourcesGenerator(stream_name="index_rate_sources",
                                          client=client_mock,
                                          config=config_json,
                                          state={'currently_syncing': 'index_rate_sources'},
                                          sub_type="self")

    assert generator.endpoint_path == 'indexratesources'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_sorting_criteria == {}
    assert generator.endpoint_filter_criteria == []
    assert generator.endpoint_params == {
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == ""
