import inspect
import os

from mock import MagicMock

from ..constants import config_json
from ..helpers import GeneratorMock

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"


def test_loan_repayments_processor_endpoint_config_init():
    from singer.catalog import Catalog
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()

    from tap_mambu.tap_mambu_refactor.tap_processors.loan_repayments_processor import LoanRepaymentsProcessor
    processor = LoanRepaymentsProcessor(catalog=catalog,
                                        stream_name="loan_repayments",
                                        client=client_mock,
                                        config=config_json,
                                        state={'currently_syncing': 'loan_repayments'},
                                        sub_type="self",
                                        generators=[GeneratorMock([])],
                                        parent_id='TEST')

    assert processor.endpoint_parent == 'loan_accounts'
    assert processor.endpoint_parent_id == 'TEST'
