from tap_mambu.tap_generators.multithreaded_offset_generator import MultithreadedOffsetGenerator
from tap_mambu.tap_generators.child_generator import ChildGenerator
from ..constants import config_json
from tap_mambu.helpers.generator_processor_pairs import get_generator_processor_for_stream
from ..helpers import ClientWithDataMock, ClientMock, ClientWithDataMultithreadedMock

MULTITHREADED_STREAMS_WITHOUT_PAGINATION_DETAILS_ON = ['activities', ]


def setup_generator_base_test(stream_name, client_mock=None, with_data=False, custom_data=None,
                              offset_field="offset", limit_field="limit", bookmark_field="creationDate"):
    generator_classes, _ = get_generator_processor_for_stream(stream_name)
    generators = list()
    for generator_class in generator_classes:
        client = client_mock
        if client_mock is None:
            client = ClientMock(int(config_json.get("page_size", 200)))
            if with_data:
                if issubclass(generator_class, MultithreadedOffsetGenerator) and \
                        stream_name not in MULTITHREADED_STREAMS_WITHOUT_PAGINATION_DETAILS_ON:
                    client = ClientWithDataMultithreadedMock(int(config_json.get("page_size", 200)),
                                                             custom_data=custom_data,
                                                             offset_field=offset_field, limit_field=limit_field,
                                                             bookmark_field=bookmark_field)
                else:
                    client = ClientWithDataMock(int(config_json.get("page_size", 200)), custom_data=custom_data,
                                                offset_field=offset_field, limit_field=limit_field,
                                                bookmark_field=bookmark_field)

        generator = generator_class(stream_name=stream_name,
                                    client=client,
                                    config=config_json,
                                    state={"currently_syncing": stream_name},
                                    sub_type="self",
                                    **({"parent_id": "TEST"} if issubclass(generator_class, ChildGenerator) else {}))
        generators.append(generator)
    return generators
