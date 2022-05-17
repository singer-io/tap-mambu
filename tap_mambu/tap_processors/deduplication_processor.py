from singer import get_logger, metrics
from singer.utils import strptime_to_utc

from .processor import TapProcessor
from ..helpers import convert
from ..helpers.exceptions import NoDeduplicationCapabilityException, NoDeduplicationKeyException

LOGGER = get_logger()


class DeduplicationProcessor(TapProcessor):
    def _init_endpoint_config(self):
        try:
            super(DeduplicationProcessor, self)._init_endpoint_config()
        except NoDeduplicationCapabilityException:
            pass
        self.endpoint_deduplication_key = "encoded_key"

        if len(self.generators) > 1:
            for generator in self.generators:
                if generator.endpoint_sorting_criteria.get("field") != self.endpoint_deduplication_key:
                    LOGGER.warning(f"(processor) Stream {self.stream_name} - Deduplication key should "
                                   "be the same as the sorting key in order for deduplication to work!")

    def _choose_next_record(self, generator_values):
        if not self.endpoint_deduplication_key:
            raise NoDeduplicationKeyException("Cannot continue deduplicating because the deduplication key is not set!")
        # Find lowest value in the list, and if two or more are equal, find max bookmark
        min_record_key = None
        min_record_value = None
        min_record_bookmark = None
        for iterator in generator_values:
            bookmark_field = convert(iterator.endpoint_bookmark_field)
            # Different record
            if min_record_key is None \
                    or min_record_value > generator_values[iterator][self.endpoint_deduplication_key]:
                min_record_key = iterator
                min_record_value = generator_values[iterator][self.endpoint_deduplication_key]
                if not bookmark_field or bookmark_field not in generator_values[iterator]:
                    continue
                min_record_bookmark = generator_values[iterator][bookmark_field]
            # Same record
            elif min_record_value == generator_values[iterator][self.endpoint_deduplication_key]:
                if not bookmark_field or bookmark_field not in generator_values[iterator]:
                    continue

                # Check the new bookmark against the min_record_key's bookmark
                min_record_dttm = strptime_to_utc(min_record_bookmark)
                record_dttm = strptime_to_utc(generator_values[iterator][bookmark_field])
                # If min_record has bookmark smaller than record, then replace min_record (we want highest bookmark)
                if min_record_dttm < record_dttm:
                    min_record_key = iterator
                    min_record_value = generator_values[iterator][self.endpoint_deduplication_key]
                    min_record_bookmark = generator_values[iterator][bookmark_field]

        return min_record_key, min_record_value

    def process_records(self):
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

                record_key, record_value = self._choose_next_record(self.generator_values)

                # Process the record
                record = self.generator_values[record_key]
                if self.process_record(record, record_key.time_extracted,
                                       record_key.endpoint_bookmark_field):
                    record_count += 1
                    record_count += self._process_child_records(record)
                    counter.increment()

                # Remove any record with the same deduplication_key from the list
                # (so we don't process the same record twice)
                for iterator in self.generator_values.keys():
                    if record_value == self.generator_values[iterator][self.endpoint_deduplication_key]:
                        self.generator_values[iterator] = None

        return record_count
