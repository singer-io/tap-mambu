import singer

from .helpers.generator_processor_pairs import get_generator_processor_for_stream

LOGGER = singer.get_logger()


def sync_endpoint_refactor(client, catalog, state,
                           stream_name, sub_type, config, parent_id=None):
    generator_classes, processor_class = get_generator_processor_for_stream(stream_name)
    generators = [generator_class(stream_name=stream_name,
                                  client=client,
                                  config=config,
                                  state=state,
                                  sub_type=sub_type,
                                  **({} if parent_id is None else {"parent_id": parent_id}))
                  for generator_class in generator_classes]
    processor = processor_class(catalog=catalog,
                                stream_name=stream_name,
                                client=client,
                                config=config,
                                state=state,
                                sub_type=sub_type,
                                generators=generators,
                                **({} if parent_id is None else {"parent_id": parent_id}))

    return processor.process_streams_from_generators()
