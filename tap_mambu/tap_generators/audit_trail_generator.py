from .generator import TapGenerator
from ..helpers import get_bookmark
from ..helpers.datetime_utils import datetime_to_utc_str, utc_now, str_to_localized_datetime
from datetime import timedelta
from ..helpers import transform_json
from singer import get_logger

LOGGER = get_logger()
AUDIT_TRAIL_WINDOW_SIZE = 30  # days
class AuditTrailGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(AuditTrailGenerator, self)._init_endpoint_config()
        self.endpoint_path = "v1/events"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v2"
        self.endpoint_api_key_type = "audit"
        self.endpoint_bookmark_field = "occurred_at"

        audit_trail_bookmark = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)

        # Backwards compatibility check, such that it doesn't fail when we use a bookmark from an older Tap version
        if type(audit_trail_bookmark) is list:
            if len(audit_trail_bookmark) != 2:
                raise ValueError("Cannot parse audit trail bookmark from list because we expect 2 values!")
            audit_trail_bookmark = audit_trail_bookmark[0]

        self.start_datetime = str_to_localized_datetime(audit_trail_bookmark)
        self.end_datetime = str_to_localized_datetime(datetime_to_utc_str(utc_now() + timedelta(days=1)))

    def _init_params(self):
        super(AuditTrailGenerator, self)._init_params()
        # Initializing has_more and next_start_time
        self.has_more = True
        self.next_start_time = None

    def get_default_start_value(self):
        audit_trail_bookmark = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)

        # Backwards compatibility check, such that it doesn't fail when we use a bookmark from an older Tap version
        if type(audit_trail_bookmark) is list:
            if len(audit_trail_bookmark) != 2:
                raise ValueError("Cannot parse audit trail bookmark from list because we expect 2 values!")
            audit_trail_bookmark = audit_trail_bookmark[0]
        
        return audit_trail_bookmark

    def _all_fetch_batch_steps(self):
        # Large buffer size can impact memory utilization of connector
        # so empty the buffer once it reaches default max limit
        if len(self.buffer) > self.max_buffer_size:
            return

        self.prepare_batch()
        LOGGER.info(f'(generator) Stream {self.stream_name} - Fetching batch with params: {self.endpoint_params}')
        response = self.fetch_batch()
        # Extracting "events" field from batch, as we are returned a dict instead of a list of records
        events_batch = response["events"]
        self.buffer = transform_json(events_batch, self.stream_name)
        if not self.buffer:
            LOGGER.warning(f'(generator) Stream {self.stream_name} - NO TRANSFORMED DATA RESULTS')

        self.has_more = response.get("hasMore")
        self.next_start_time = response.get("nextStartTime")
        LOGGER.info(f'(generator) Stream {self.stream_name} - hasMore: {self.has_more}, nextStartTime: {self.next_start_time}')

    def next(self):
        if not self.buffer:
            if self.has_more or self.start_datetime < self.end_datetime:
                raise StopIteration()
            self._all_fetch_batch_steps()
            if not self.buffer:
                raise StopIteration()
        return self.buffer.pop(0)

    def prepare_batch(self):
        # Update the params to fetch the next batch
        if self.next_start_time:
            self.endpoint_params["occurred_at[gte]"] = datetime_to_utc_str(str_to_localized_datetime(self.next_start_time))
        else:
            current_window_end_datetime = self.start_datetime + timedelta(days=AUDIT_TRAIL_WINDOW_SIZE)
            self.endpoint_params = {
                "occurred_at[gte]": datetime_to_utc_str(self.start_datetime - timedelta(minutes=5)),
                "occurred_at[lte]": datetime_to_utc_str(current_window_end_datetime)
            }
            self.start_datetime = current_window_end_datetime
