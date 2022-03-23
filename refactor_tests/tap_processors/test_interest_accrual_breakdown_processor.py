import inspect
import os

from mock import MagicMock

from ..constants import config_json
from ..helpers import GeneratorMock

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"


def test_interest_accrual_breakdown_processor():
    from singer.catalog import Catalog
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()

    from tap_mambu.tap_mambu_refactor.tap_processors.interest_accrual_breakdown import InterestAccrualBreakdownProcessor
    processor = InterestAccrualBreakdownProcessor(catalog=catalog,
                                                  stream_name="interest_accrual_breakdown",
                                                  client=client_mock,
                                                  config=config_json,
                                                  state={'currently_syncing': 'interest_accrual_breakdown'},
                                                  sub_type="self",
                                                  generators=[GeneratorMock([])])

    assert processor.endpoint_deduplication_key == "entry_id"
