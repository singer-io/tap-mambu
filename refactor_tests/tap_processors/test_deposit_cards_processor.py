import inspect
import os

from mock import MagicMock

from ..constants import config_json
from ..helpers import GeneratorMock

FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"


def test_deposit_cards_processor_endpoint_config_init():
    from singer.catalog import Catalog
    catalog = Catalog.load(f"{FIXTURES_PATH}/processor_catalog.json")
    client_mock = MagicMock()

    from tap_mambu.tap_mambu_refactor.tap_processors.deposit_cards_processor import DepositCardsProcessor
    processor = DepositCardsProcessor(catalog=catalog,
                                      stream_name="cards",
                                      client=client_mock,
                                      config=config_json,
                                      state={'currently_syncing': 'cards'},
                                      sub_type="self",
                                      generators=[GeneratorMock([])],
                                      parent_id='TEST')

    assert processor.endpoint_deduplication_key == 'reference_token'
    assert processor.endpoint_id_field == 'reference_token'
    assert processor.endpoint_parent == 'deposit'
    assert processor.endpoint_parent_id == 'TEST'
