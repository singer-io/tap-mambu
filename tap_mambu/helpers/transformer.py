from singer.transform import Transformer as SingerTransformer

from tap_mambu.helpers.datetime_utils import datetime_to_utc_str, str_to_localized_datetime


class Transformer(SingerTransformer):
    def _transform_datetime(self, value):
        return datetime_to_utc_str(str_to_localized_datetime(value))


def transform_datetime(this_dttm):
    with Transformer() as transformer:
        # noinspection PyProtectedMember
        # pylint: disable=W0212
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm
