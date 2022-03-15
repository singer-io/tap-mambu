from mock import MagicMock

from ..constants import config_json


def test_deposit_transactions_generator():
    from tap_mambu.tap_mambu_refactor.tap_generators.deposit_transactions_generator import DepositTransactionsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = DepositTransactionsGenerator(stream_name="deposit_transactions",
                                             client=client_mock,
                                             config=config_json,
                                             state={'currently_syncing': 'deposit_transactions'},
                                             sub_type="self")
    assert generator.endpoint_path == "deposits/transactions:search"
    assert generator.endpoint_bookmark_field == "creationDate"
    assert generator.endpoint_sorting_criteria == {
            "field": "creationDate",
            "order": "ASC"
        }
    assert generator.endpoint_filter_criteria == [
            {
                "field": "creationDate",
                "operator": "AFTER",
                "value": '2021-06-01'
            }
        ]