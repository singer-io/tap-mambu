from abc import ABC

from singer import write_record, Transformer, metadata, write_schema
from singer.utils import strptime_to_utc

from ..Helpers import transform_datetime, convert


class TapProcessor(ABC):
    def __init__(self, catalog, stream_name):
        self.generators = list()
        self.generator_values = dict()
        self.catalog = catalog
        self.stream_name = stream_name
        self.stream = self.catalog.get_stream(stream_name)
        self.schema = self.stream.schema.to_dict()
        self.stream_metadata = metadata.to_map(self.stream.metadata)
        self.deduplication_key = "id"

    def write_schema(self):
        stream = self.catalog.get_stream(self.stream_name)
        schema = stream.schema.to_dict()
        write_schema(self.stream_name, schema, stream.key_properties)

    def configure_processor_for_generators(self):
        id_fields = self.generators[0].endpoint_config['id_fields']
        if "id" not in id_fields:
            self.deduplication_key = id_fields[0]

    def process_streams_from_generators(self, generators):
        self.generators = generators
        self.configure_processor_for_generators()
        self.write_schema()
        record_count = 0
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
            # Find lowest value in the list, and if two or more are equal, find max bookmark
            min_record_key = None
            min_record_value = None
            min_record_bookmark = None
            for iterator in self.generator_values:
                bookmark_field = convert(iterator.endpoint_config.get('bookmark_field', ''))
                # Different record
                if min_record_key is None \
                        or min_record_value > self.generator_values[iterator][self.deduplication_key]:
                    min_record_key = iterator
                    min_record_value = self.generator_values[iterator][self.deduplication_key]
                    if not bookmark_field:
                        continue
                    min_record_bookmark = self.generator_values[iterator][bookmark_field]
                # Same record
                elif min_record_value == self.generator_values[iterator][self.deduplication_key]:
                    if not bookmark_field:
                        continue

                    # Check the new bookmark against the min_record_key's bookmark
                    min_record_dttm = strptime_to_utc(min_record_bookmark)
                    record_dttm = strptime_to_utc(self.generator_values[iterator][bookmark_field])
                    # If min_record has bookmark smaller than record, then replace min_record (we want highest bookmark)
                    if min_record_dttm < record_dttm:
                        min_record_key = iterator
                        min_record_value = self.generator_values[iterator][self.deduplication_key]
                        min_record_bookmark = self.generator_values[iterator][bookmark_field]

            # Process the record
            record = self.generator_values[min_record_key]
            if self.process_record(record, min_record_key.time_extracted,
                                   min_record_key.endpoint_config.get('bookmark_field', '')):
                record_count += 1
                record_count += self._process_child_records(record)

            # Remove any record with the same deduplication_key from the list
            # (so we don't process the same record twice)
            for iterator in self.generator_values.keys():
                if min_record_value == self.generator_values[iterator][self.deduplication_key]:
                    self.generator_values[iterator] = None

        self.generators[0].write_bookmark()
        return record_count

    def _process_child_records(self, record):
        return 0

    def __is_record_past_bookmark(self, transformed_record, bookmark_field):
        is_record_past_bookmark = False
        bookmark_type = self.generators[0].endpoint_config.get('bookmark_type')
        bookmark_field = convert(bookmark_field)

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

    def process_record(self, record, time_extracted, bookmark_field):
        with Transformer() as transformer:
            transformed_record = transformer.transform(record,
                                                       self.schema,
                                                       self.stream_metadata)

        if self.__is_record_past_bookmark(transformed_record, bookmark_field):
            write_record(self.stream_name,
                         transformed_record,
                         time_extracted=time_extracted)
            return True
        return False
