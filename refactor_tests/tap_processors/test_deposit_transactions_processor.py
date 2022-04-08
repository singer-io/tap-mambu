from . import setup_processor_base_test


def test_clients_processor():
    processor = setup_processor_base_test("deposit_transactions")

    assert processor.endpoint_deduplication_key == "encoded_key"
