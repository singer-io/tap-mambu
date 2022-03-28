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
        audit_trail_bookmark = get_bookmark(self.state, self.stream_name, self.sub_type, self.start_date)
        self.audit_trail_offset = 0
        if type(audit_trail_bookmark) is list:
            audit_trail_bookmark = audit_trail_bookmark[0]
            self.audit_trail_offset = 1
        self.endpoint_params = {}
        self.endpoint_params["sort_order"] = "asc"
        self.endpoint_params["occurred_at[gte]"] = transform_datetime(audit_trail_bookmark)
        self.endpoint_params["occurred_at[lte]"] = utils.strftime(utils.now())
        self.endpoint_bookmark_field = "occurred_at"

    def _init_params(self):
        super(AuditTrailGenerator, self)._init_params()
        self.offset = self.audit_trail_offset

    def next(self):
        if not self.buffer:
            if self.last_batch_size < self.limit:
                raise StopIteration()
            self.offset = 1
            self._TapGenerator__all_fetch_batch_steps()
            if not self.buffer:
                raise StopIteration()
        return self.buffer.pop(0)

    def prepare_batch(self):
        self.params = {
            "from": self.offset,
            "size": self.limit,
            **self.static_params
        }

    def fetch_batch(self):
        return super(AuditTrailGenerator, self).fetch_batch()["events"]
