from singer import utils
from .generator import TapGenerator
from ..helpers import transform_datetime, get_bookmark


class AuditTrailGenerator(TapGenerator):
    def _init_endpoint_config(self):
        super(AuditTrailGenerator, self)._init_endpoint_config()
        self.endpoint_path = "v1/events"
        self.endpoint_api_method = "GET"
        self.endpoint_api_version = "v1"
        self.endpoint_api_key_type = "audit"
        self.endpoint_bookmark_field = "occurred_at"

        audit_trail_bookmark = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        self.audit_trail_offset = 0

        # Backwards compatibility check, such that it doesn't fail when we use a bookmark from an older Tap version
        if type(audit_trail_bookmark) is list:
            if len(audit_trail_bookmark) != 2:
                raise ValueError("Cannot parse audit trail bookmark from list because we expect 2 values!")
            self.audit_trail_offset = audit_trail_bookmark[1]
            audit_trail_bookmark = audit_trail_bookmark[0]

        self.endpoint_params = {
            "sort_order": "asc",
            "occurred_at[gte]": transform_datetime(audit_trail_bookmark),
            "occurred_at[lte]": utils.strftime(utils.now()),
        }

    def _init_params(self):
        super(AuditTrailGenerator, self)._init_params()
        # Initializing offset with value from bookmark, so we skip N records that were extracted after the bookmark
        self.offset = self.audit_trail_offset

    def next(self):
        if not self.buffer:
            if self.last_batch_size < self.limit:
                raise StopIteration()
            # Overriding offset, as every subsequent request will include exactly one record that was already extracted
            self.offset = 1
            self._all_fetch_batch_steps()
            if not self.buffer:
                raise StopIteration()
        return self.buffer.pop(0)

    def prepare_batch(self):
        # Overriding params, as they have different names in audit trail
        # WARNING: self.static_params will be changed by the processor dinamically!
        self.params = {
            "from": self.offset,
            "size": self.limit,
            **self.static_params
        }

    def fetch_batch(self):
        # Extracting "events" field from batch, as we are returned a dict instead of a list of records
        return super(AuditTrailGenerator, self).fetch_batch()["events"]
