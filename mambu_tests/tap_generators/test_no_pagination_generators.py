from unittest.mock import MagicMock

from mambu_tests.constants import config_json
from mambu_tests.tap_generators import setup_generator_base_test
from tap_mambu.helpers.generator_processor_pairs import get_available_streams
from tap_mambu.tap_generators.no_pagination_generator import NoPaginationGenerator


def test_no_pagination_generators():
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 1))

    for stream in get_available_streams():
        generators = setup_generator_base_test(stream, client_mock=client_mock)

        for generator in generators:
            if not isinstance(generator, NoPaginationGenerator):
                continue
            return_values = list()

            client_mock.request = MagicMock()
            client_mock.request.return_value = [{} for _ in range(2)]

            for val in generator:
                return_values.append(val)
                client_mock.request.assert_called_once_with(
                    method=generator.endpoint_api_method,
                    path=generator.endpoint_path,
                    version=generator.endpoint_api_version,
                    apikey_type=generator.endpoint_api_key_type,
                    params="",
                    endpoint=stream,
                    json={"sortingCriteria": {},
                          "filterCriteria": []}
                )
            assert len(client_mock.request.return_value) == len(return_values)
