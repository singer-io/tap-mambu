from abc import ABC

from singer import write_record, metadata, write_schema, get_logger, metrics, utils

from ..helpers import convert, get_bookmark, write_bookmark
from ..helpers.transformer import Transformer
from ..helpers.exceptions import NoDeduplicationCapabilityException
from ..helpers.perf_metrics import PerformanceMetrics
from ..helpers.datetime_utils import utc_now, str_to_datetime, datetime_to_utc_str, str_to_localized_datetime

LOGGER = get_logger()


class TapProcessor(ABC):
    def __init__(self, catalog, stream_name, client, config, state, sub_type, generators):
        self.start_time = utc_now()
        self.generators = generators
        self.generator_values = dict()
        for generator in self.generators:
            self.generator_values[generator.__iter__()] = None
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

    def _init_config(self):
        self.start_date = self.config.get('start_date')

    def _init_endpoint_config(self):
        self.endpoint_child_streams = []
        self.endpoint_id_field = "id"

        if len(self.generators) > 1:
            raise NoDeduplicationCapabilityException("In order to merge streams in the processor, "
                                                     "you need to use the deduplication processor")

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

    def process_records(self):
        record_count = 0
        with metrics.record_counter(self.stream_name) as counter:
            for record in self.generators[0]:
                # Process the record
                with PerformanceMetrics(metric_name="processor"):
                    is_processed = self.process_record(record, utils.now(),
                                                       self.generators[0].endpoint_bookmark_field)
                if is_processed:
                    record_count += 1
                    self._process_child_records(record)
                    counter.increment()
        return record_count

    def process_streams_from_generators(self):
        self.write_schema()

        record_count = self.process_records()
        self.write_bookmark()
        return record_count

    # This function is provided for processors with child streams, must be overridden if child streams are to be used
    def _process_child_records(self, record):
        pass

    def _update_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)
        if bookmark_field and (bookmark_field in transformed_record):
            bookmark_dttm = str_to_datetime(transformed_record[bookmark_field])
            max_bookmark_value_dttm = str_to_datetime(self.max_bookmark_value)
            if bookmark_dttm > max_bookmark_value_dttm:
                self.max_bookmark_value = datetime_to_utc_str(min(bookmark_dttm, self.start_time))

    def _is_record_past_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)

        # If stream doesn't have a bookmark field or the record doesn't contain the stream's bookmark field
        if not bookmark_field or (bookmark_field not in transformed_record):
            return True

        # Keep only records whose bookmark is after the last_datetime
        if str_to_localized_datetime(transformed_record[bookmark_field]) >= \
                str_to_localized_datetime(self.last_bookmark_value):
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
