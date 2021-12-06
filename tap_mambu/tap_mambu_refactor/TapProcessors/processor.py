from abc import ABC

from singer import write_record, Transformer, metadata, write_schema
from singer.utils import strptime_to_utc

from ..Helpers import transform_datetime, convert
from ..TapGenerators.generator import TapGenerator


class TapProcessor(ABC):
    def __init__(self, catalog, stream_name):
        self.generators = list()
        self.generator_values = dict()
        self.catalog = catalog
        self.stream_name = stream_name
        self.deduplication_key = "encoded_key"
        self.stream = self.catalog.get_stream(stream_name)
        self.schema = self.stream.schema.to_dict()
        self.stream_metadata = metadata.to_map(self.stream.metadata)

    def write_schema(self):
        stream = self.catalog.get_stream(self.stream_name)
        schema = stream.schema.to_dict()
        write_schema(self.stream_name, schema, stream.key_properties)

    def process_streams_from_generators(self, generators):
        self.generators = generators
        self.write_schema()
        for generator in self.generators:
            self.generator_values[iter(generator)] = None
        while True:
            # Populate list of values from generators (if any were removed)
            for iterator in list(self.generator_values):
                if self.generator_values[iterator] is None:
                    self.generator_values[iterator] = next(iterator, None)
                if self.generator_values[iterator] is None:
                    self.generator_values.pop(iterator)
            if not self.generator_values:
                break
            # Find lowest value in the list
            min_record_key: TapGenerator = None
            min_record_value = None
            for iterator in self.generator_values:
                if min_record_value is None \
                        or min_record_value > self.generator_values[iterator][self.deduplication_key]:
                    min_record_key = iterator
                    min_record_value = self.generator_values[iterator][self.deduplication_key]

            # Process the record
            record = self.generator_values[min_record_key]
            self.process_record(record, min_record_key.time_extracted)

            # Remove any record with the same deduplication_key from the list
            # (so we don't process the same record twice)
            for iterator in self.generator_values.keys():
                if min_record_value == self.generator_values[iterator][self.deduplication_key]:
                    self.generator_values[iterator] = None

        self.generators[0].write_bookmark()

    def __is_record_past_bookmark(self, transformed_record):
        is_record_past_bookmark = False
        bookmark_type = self.generators[0].endpoint_config.get('bookmark_type')
        bookmark_field = convert(self.generators[0].endpoint_config.get('bookmark_field'))

        # Reset max_bookmark_value to new value if higher
        if bookmark_field and (bookmark_field in transformed_record):
            bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
            if hasattr(self.generators[0], "max_bookmark_value"):
                max_bookmark_value_dttm = strptime_to_utc(self.generators[0].max_bookmark_value)
                if bookmark_dttm > max_bookmark_value_dttm:
                    self.generators[0].max_bookmark_value = transformed_record[bookmark_field]
            else:
                self.generators[0].max_bookmark_value = transformed_record[bookmark_field]

        if bookmark_field and (bookmark_field in transformed_record):
            if bookmark_type == 'integer':
                # Keep only records whose bookmark is after the last_integer
                if transformed_record[bookmark_field] >= self.generators[0].last_bookmark_value:
                    is_record_past_bookmark = True
            elif bookmark_type == 'datetime':
                last_dttm = transform_datetime(self.generators[0].last_bookmark_value)
                bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                # Keep only records whose bookmark is after the last_datetime
                if bookmark_dttm >= last_dttm:
                    is_record_past_bookmark = True
        else:
            is_record_past_bookmark = True

        return is_record_past_bookmark

    def process_record(self, record, time_extracted):
        with Transformer() as transformer:
            transformed_record = transformer.transform(record,
                                                       self.schema,
                                                       self.stream_metadata)

        if self.__is_record_past_bookmark(transformed_record):
            write_record(self.stream_name,
                         transformed_record,
                         time_extracted=time_extracted)
