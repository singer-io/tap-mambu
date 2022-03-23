from . import setup_processor_base_test


def test_credit_arrangements_processor():
    processor = setup_processor_base_test("credit_arrangements")

    assert processor.endpoint_deduplication_key == "id"
