from mock import MagicMock

from tap_mambu.tap_generators.child_generator import ChildGenerator
from ..constants import config_json
from tap_mambu.helpers.generator_processor_pairs import get_generator_processor_for_stream


def setup_generator_base_test(stream_name, client_mock=None):
    if client_mock is None:
        client_mock = MagicMock()
        client_mock.page_size = int(config_json.get("page_size", 500))
        client_mock.request = MagicMock()
    generator_classes, _ = get_generator_processor_for_stream(stream_name)

    generators = list()
    for generator_class in generator_classes:
        generator = generator_class(stream_name=stream_name,
                                    client=client_mock,
                                    config=config_json,
                                    state={"currently_syncing": stream_name},
                                    sub_type="self",
                                    **({"parent_id": "TEST"} if issubclass(generator_class, ChildGenerator) else {}))
        generators.append(generator)
    return generators
