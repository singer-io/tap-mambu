from unittest.mock import MagicMock

from ..constants import config_json


def test_deposit_cards_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.deposit_cards_generator import DepositCardsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = DepositCardsGenerator(stream_name="deposit_accounts",
                                      client=client_mock,
                                      config=config_json,
                                      state={'currently_syncing': 'deposit_accounts'},
                                      sub_type="self",
                                      parent_id="TEST")

    assert generator.endpoint_parent_id == "TEST"
    assert generator.endpoint_path == 'deposits/TEST/cards'
    assert generator.endpoint_api_method == "GET"
