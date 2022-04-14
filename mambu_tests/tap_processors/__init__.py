from mock import MagicMock

from tap_mambu.tap_processors.child_processor import ChildProcessor
from ..constants import config_json
from ..helpers import GeneratorMock
from tap_mambu.helpers.generator_processor_pairs import get_generator_processor_for_stream


def setup_processor_base_test(stream_name):
    from tap_mambu import discover
    catalog = discover()
    client_mock = MagicMock()
    _, processor_class = get_generator_processor_for_stream(stream_name)

    processor = processor_class(catalog=catalog,
                                stream_name=stream_name,
                                client=client_mock,
                                config=config_json,
                                state={"currently_syncing": stream_name},
                                sub_type="self",
                                generators=[GeneratorMock([])],
                                **({"parent_id": "TEST"} if issubclass(processor_class, ChildProcessor) else {}))
    return processor
