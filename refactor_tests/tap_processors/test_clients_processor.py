from . import setup_processor_base_test


def test_clients_processor():
    processor = setup_processor_base_test("clients")

    assert processor.endpoint_deduplication_key == "id"
