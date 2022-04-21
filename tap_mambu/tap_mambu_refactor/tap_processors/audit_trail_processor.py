from .processor import TapProcessor
from singer.utils import strptime_to_utc
from ..helpers import convert, write_bookmark, get_bookmark


class AuditTrailProcessor(TapProcessor):
    def __init__(self, *args, **kwargs):
        super(AuditTrailProcessor, self).__init__(*args, **kwargs)
        # This will be used to signal the generator how many records to skip (when more than one record has same date)
        self.bookmark_offset = 1

    def _init_endpoint_config(self):
        # There are no unique fields in this stream
        self.endpoint_deduplication_key = None

    def _init_bookmarks(self):
        super(AuditTrailProcessor, self)._init_bookmarks()
        self.last_bookmark_value = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        if type(self.last_bookmark_value) is list:
            if len(self.last_bookmark_value) != 2:
                raise ValueError("Cannot parse audit trail bookmark from list because we expect 2 values!")
            self.last_bookmark_value = self.last_bookmark_value[0]
        self.max_bookmark_value = self.last_bookmark_value

    def _update_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)
        if bookmark_field and (bookmark_field in transformed_record):
            bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
            max_bookmark_value_dttm = strptime_to_utc(self.max_bookmark_value)
            if bookmark_dttm > max_bookmark_value_dttm:
                self.max_bookmark_value = bookmark_dttm.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                # Reset identical record count
                self.bookmark_offset = 0

            max_bookmark_value_dttm = strptime_to_utc(self.max_bookmark_value)
            if max_bookmark_value_dttm == bookmark_dttm:
                # Increment identical record count
                self.bookmark_offset += 1

        for generator in self.generators:
            # Bad practice, but necessary as we need to change extraction parameters while extracting
            generator.static_params["occurred_at[gte]"] = self.max_bookmark_value

    def write_bookmark(self):
        if all([generator.endpoint_bookmark_field for generator in self.generators]):
            write_bookmark(self.state,
                           self.stream_name,
                           self.sub_type,
                           [self.max_bookmark_value, self.bookmark_offset])
            # Saving bookmark as a list, containing the bookmark value, as well as the number of records extracted on
            # the same date as the bookmark so we skip them at the next extraction
