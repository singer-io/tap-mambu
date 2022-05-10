from singer.transform import Transformer as SingerTransformer

from tap_mambu.helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime


class Transformer(SingerTransformer):
    def _transform_datetime(self, value):
        return datetime_to_utc_str(str_to_localized_datetime(value))
