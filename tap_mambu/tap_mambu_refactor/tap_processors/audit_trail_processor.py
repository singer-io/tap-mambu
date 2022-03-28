from .processor import TapProcessor
from singer.utils import strptime_to_utc
from ..helpers import convert, write_bookmark


class AuditTrailProcessor(TapProcessor):
    def __init__(self, *args, **kwargs):
        super(AuditTrailProcessor, self).__init__(*args, **kwargs)
        self.bookmark_offset = 1

    def _init_endpoint_config(self):
        self.endpoint_deduplication_key = "occurred_at"

    def _update_bookmark(self, transformed_record, bookmark_field):
        bookmark_field = convert(bookmark_field)
        if bookmark_field and (bookmark_field in transformed_record):
            bookmark_dttm = strptime_to_utc(transformed_record[bookmark_field])
            max_bookmark_value_dttm = strptime_to_utc(self.max_bookmark_value)
            if bookmark_dttm > max_bookmark_value_dttm:
                self.max_bookmark_value = bookmark_dttm.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                self.bookmark_offset = 0

            max_bookmark_value_dttm = strptime_to_utc(self.max_bookmark_value)
            if max_bookmark_value_dttm == bookmark_dttm:
                self.bookmark_offset += 1

        for generator in self.generators:
            generator.static_params["occurred_at[gte]"] = self.max_bookmark_value

    def write_bookmark(self):
        if all([generator.endpoint_bookmark_field for generator in self.generators]):
            write_bookmark(self.state,
                           self.stream_name,
                           self.sub_type,
                           [self.max_bookmark_value, self.bookmark_offset])
