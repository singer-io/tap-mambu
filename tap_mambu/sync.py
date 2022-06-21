import singer

from .helpers import get_selected_streams, should_sync_stream, update_currently_syncing
from .helpers.generator_processor_pairs import get_generator_processor_for_stream, get_stream_subtypes
from .helpers.perf_metrics import PerformanceMetrics

LOGGER = singer.get_logger()


def sync_endpoint(client, catalog, state,
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


def sync_all_streams(client, config, catalog, state):
    from .tap_generators.child_generator import ChildGenerator
    from .tap_processors.child_processor import ChildProcessor

    PerformanceMetrics.set_generator_batch_size(int(config.get("page_size", 500)))
    
    selected_streams = get_selected_streams(catalog)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))

    # Start syncing from last/currently syncing stream
    if last_stream in selected_streams:
        selected_streams = selected_streams[selected_streams.index(last_stream):] +\
                           selected_streams[:selected_streams.index(last_stream)]

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name in selected_streams:
        # Check if stream is child stream (and ignore it)
        generators, processor = get_generator_processor_for_stream(stream_name)
        if any([issubclass(generator, ChildGenerator) for generator in generators]) or \
                issubclass(processor, ChildProcessor):
            continue

        should_stream, last_stream = should_sync_stream(selected_streams,
                                                        last_stream,
                                                        stream_name)

        if should_stream:
            # loop through each sub type
            sub_types = get_stream_subtypes(stream_name)
            for sub_type in sub_types:
                LOGGER.info('START Syncing: {}, Type: {}'.format(stream_name, sub_type))

                update_currently_syncing(state, stream_name)
                PerformanceMetrics.reset_metrics()
                total_records = sync_endpoint(
                    client=client,
                    catalog=catalog,
                    state=state,
                    stream_name=stream_name,
                    sub_type=sub_type,
                    config=config
                )

                update_currently_syncing(state, None)
                LOGGER.info('Synced: {}, total_records: {}'.format(
                                stream_name,
                                total_records))
                LOGGER.info('FINISHED Syncing: {}'.format(stream_name))

                statistics = PerformanceMetrics.get_statistics()

                if statistics['generator'] and statistics['generator_98th']:
                    LOGGER.info(f"Average Generator Records/s: {round(1/statistics['generator'])} "
                                f"[98th percentile: {round(1/statistics['generator_98th'])}]")

                if statistics['processor'] and statistics['processor_98th']:
                    LOGGER.info(f"Average Processor Records/s: {round(1/statistics['processor'])} "
                                f"[98th percentile: {round(1/statistics['processor_98th'])}]")

                LOGGER.info(f"Total Generator Wait (s): {round(statistics['generator_wait'], 1)} ")

                LOGGER.info(f"Total Processor Wait (s): {round(statistics['processor_wait'], 1)} ")

                LOGGER.info(f"Average Records/s: {statistics['records']}")
                LOGGER.info(f"Total Duration: {statistics['extraction']}")
