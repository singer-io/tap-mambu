import json
from singer import write_record, Transformer, metadata, write_schema
from singer.utils import strptime_to_utc

from ..TapGenerators.generator import TapGenerator


class TapProcessor:
    def __init__(self, generator: TapGenerator, catalog, stream_name, endpoint_config):
        self.generator = generator
        self.catalog = catalog
        self.stream_name = stream_name
        self.endpoint_config = endpoint_config
        self.stream = self.catalog.get_stream(stream_name)
        self.schema = self.stream.schema.to_dict()
        self.stream_metadata = metadata.to_map(self.stream.metadata)

    def write_schema(self):
        stream = self.catalog.get_stream(self.stream_name)
        schema = stream.schema.to_dict()
        write_schema(self.stream_name, schema, stream.key_properties)

    def process_stream_from_generator(self):
        self.write_schema()
        for record in self.generator:
            self.process_record(record)

    def __is_record_past_bookmark(self, transformed_record):
        bookmark_type = self.endpoint_config.get('bookmark_type')
        bookmark_field = self.endpoint_config.get('bookmark_field')
        if type(bookmark_field) is list:
            bookmark_found = False
            for bookmark in bookmark_field:
                if bookmark and (bookmark in transformed_record):
                    bookmark_dttm = strptime_to_utc(transformed_record[bookmark])
                    if self.generator.max_bookmark_value:
                        max_bookmark_value_dttm = strptime_to_utc(self.generator.max_bookmark_value)
                        if bookmark_dttm > max_bookmark_value_dttm:
                            self.generator.max_bookmark_value = transformed_record[bookmark]
                    else:
                        self.generator.max_bookmark_value = transformed_record[bookmark]

                if bookmark and (bookmark in transformed_record):
                    bookmark_found = True
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark] >= self.generator.last_bookmark_value:
                            return True
                    elif bookmark_type == 'datetime':
                        with Transformer() as transformer:
                            last_dttm = transformer._transform_datetime(self.generator.last_bookmark_value)
                        with Transformer() as transformer:
                            bookmark_dttm = transformer._transform_datetime(transformed_record[bookmark])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            return True
                            index = (bookmark_field.index(bookmark)) + 1
                            # Check if the rest of the bookmarks have a value higher than the current max_bookmark
                            for bookmark in bookmark_field[index:]:
                                if bookmark and (bookmark in transformed_record):
                                    bookmark_dttm = strptime_to_utc(transformed_record[bookmark])
                                    max_bookmark_value_dttm = strptime_to_utc(self.generator.max_bookmark_value)
                                    if bookmark_dttm > max_bookmark_value_dttm:
                                        self.generator.max_bookmark_value = transformed_record[bookmark]

                            break
            if not bookmark_found:
                return True
        return False

    def process_record(self, record):
        with Transformer() as transformer:
            transformed_record = transformer.transform(record,
                                                       self.schema,
                                                       self.stream_metadata)

        if self.__is_record_past_bookmark(transformed_record):
            write_record(self.stream_name,
                         transformed_record,
                         time_extracted=self.generator.time_extracted)
