from abc import ABC

from singer import write_record, Transformer, metadata, write_schema, get_logger, metrics
from singer.utils import strptime_to_utc, now as singer_now

from ..helpers import transform_datetime, convert, get_bookmark, write_bookmark

LOGGER = get_logger()


class TapProcessor(ABC):
    def __init__(self, catalog, stream_name, client, config, state, sub_type, generators):
        self.start_time = singer_now()
        self.generators = generators
        self.generator_values = dict()
        for generator in self.generators:
            self.generator_values[iter(generator)] = None
        self.catalog = catalog
        self.stream_name = stream_name
        self.client = client
        self.config = config
        self.state = state
        self.sub_type = sub_type
        self.stream = self.catalog.get_stream(stream_name)
        self.schema = self.stream.schema.to_dict()
        self.stream_metadata = metadata.to_map(self.stream.metadata)
        self._init_config()
        self._init_endpoint_config()
        self._init_bookmarks()
        LOGGER.info(f'(processor) Stream {self.stream_name} - bookmark value used: {self.last_bookmark_value}')

        if len(self.generators) > 1:
            for generator in self.generators:
                if generator.endpoint_sorting_criteria.get("field") != self.endpoint_deduplication_key:
                    LOGGER.warning(f"(processor) Stream {self.stream_name} - Deduplication key should "
                                   "be the same as the sorting key in order for deduplication to work!")

    def _init_config(self):
        self.start_date = self.config.get('start_date')

    def _init_endpoint_config(self):
        self.endpoint_deduplication_key = "encoded_key"
        self.endpoint_child_streams = []
        self.endpoint_id_field = "id"

    def _init_bookmarks(self):
        self.last_bookmark_value = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        self.max_bookmark_value = self.last_bookmark_value

    def write_schema(self):
        stream = self.catalog.get_stream(self.stream_name)
        schema = stream.schema.to_dict()
        try:
            write_schema(self.stream_name, schema, stream.key_properties)
        except OSError as err:
            LOGGER.info(f'OS Error writing schema for: {self.stream_name}')
            raise err

    def process_streams_from_generators(self):
        self.write_schema()
        record_count = 0
        with metrics.record_counter(self.stream_name) as counter:
            while True:
                # Populate list of values from generators (if any were removed)
                for iterator in list(self.generator_values):
                    if self.generator_values[iterator] is None:
                        self.generator_values[iterator] = next(iterator, None)
                    if self.generator_values[iterator] is None:
                        self.generator_values.pop(iterator)
                if not self.generator_values:
                    break
                # Find lowest value in the list, and if two or more are equal, find max bookmark
                min_record_key = None
                min_record_value = None
                min_record_bookmark = None
                for iterator in self.generator_values:
                    bookmark_field = convert(iterator.endpoint_bookmark_field)
                    # Different record
                    if min_record_key is None \
                            or min_record_value > self.generator_values[iterator][self.endpoint_deduplication_key]:
                        min_record_key = iterator
                        min_record_value = self.generator_values[iterator][self.endpoint_deduplication_key]
                        if not bookmark_field or bookmark_field not in self.generator_values[iterator]:
                            continue
                        min_record_bookmark = self.generator_values[iterator][bookmark_field]
                    # Same record
                    elif min_record_value == self.generator_values[iterator][self.endpoint_deduplication_key]:
                        if not bookmark_field or bookmark_field not in self.generator_values[iterator]:
                            continue

                        # Check the new bookmark against the min_record_key's bookmark
                        min_record_dttm = strptime_to_utc(min_record_bookmark)
                        record_dttm = strptime_to_utc(self.generator_values[iterator][bookmark_field])
                        # If min_record has bookmark smaller than record, then replace min_record (we want highest bookmark)
                        if min_record_dttm < record_dttm:
                            min_record_key = iterator
                            min_record_value = self.generator_values[iterator][self.endpoint_deduplication_key]
                            min_record_bookmark = self.generator_values[iterator][bookmark_field]

                # Process the record
                record = self.generator_values[min_record_key]
                if self.process_record(record, min_record_key.time_extracted,
                                       min_record_key.endpoint_bookmark_field):
                    record_count += 1
                    record_count += self._process_child_records(record)
                    counter.increment()

                # Remove any record with the same deduplication_key from the list
                # (so we don't process the same record twice)
                for iterator in self.generator_values.keys():
                    if min_record_value == self.generator_values[iterator][self.endpoint_deduplication_key]:
                        self.generator_values[iterator] = None

        self.write_bookmark()
        return record_count

    def _process_child_records(self, record):
        return 0

    def _update_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)
        if bookmark_field and (bookmark_field in transformed_record):
            bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
            max_bookmark_value_dttm = strptime_to_utc(self.max_bookmark_value)
            if bookmark_dttm > max_bookmark_value_dttm:
                self.max_bookmark_value = min(bookmark_dttm,
                                              self.start_time).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def _is_record_past_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)

        # If stream doesn't have a bookmark field or the record doesn't contain the stream's bookmark field
        if not bookmark_field or (bookmark_field not in transformed_record):
            return True

        # Keep only records whose bookmark is after the last_datetime
        if transform_datetime(transformed_record[bookmark_field]) >= transform_datetime(self.last_bookmark_value):
            return True
        return False

    def process_record(self, record, time_extracted, bookmark_field):
        with Transformer() as transformer:
            transformed_record = transformer.transform(record,
                                                       self.schema,
                                                       self.stream_metadata)

        self._update_bookmark(transformed_record, bookmark_field)
        if self._is_record_past_bookmark(transformed_record, bookmark_field):
            try:
                write_record(self.stream_name,
                             transformed_record,
                             time_extracted=time_extracted)
            except OSError as err:
                LOGGER.info(f'OS Error writing record for: {self.stream_name}')
                LOGGER.info(f'record: {transformed_record}')
                raise err

            return True
        return False

    def write_bookmark(self):
        if all([generator.endpoint_bookmark_field for generator in self.generators]):
            write_bookmark(self.state,
                           self.stream_name,
                           self.sub_type,
                           self.max_bookmark_value)
