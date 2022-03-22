from unittest.mock import MagicMock

from ..constants import config_json


def test_deposit_cards_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.deposit_cards_generator import DepositCardsGenerator
    client_mock = MagicMock()
    client_mock.page_size = 1
    client_mock.request = MagicMock()
    client_mock.request.return_value = [{} for _ in range(2)]
    generator = DepositCardsGenerator(stream_name="cards",
                                      client=client_mock,
                                      config=config_json,
                                      state={'currently_syncing': 'cards'},
                                      sub_type="self",
                                      parent_id="TEST")

    assert generator.endpoint_parent_id == "TEST"
    assert generator.endpoint_path == 'deposits/TEST/cards'
    assert generator.endpoint_api_method == "GET"

    return_values = list()
    for val in generator:
        return_values.append(val)
        client_mock.request.assert_called_once_with(
            method="GET",
            path='deposits/TEST/cards',
            version="v2",
            apikey_type=None,
            params="",
            endpoint="cards",
            json={"sortingCriteria": {},
                  "filterCriteria": []}
        )
    assert len(client_mock.request.return_value) == len(return_values)
