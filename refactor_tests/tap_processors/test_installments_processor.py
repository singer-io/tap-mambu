from . import setup_processor_base_test


def test_installments_processor():
    processor = setup_processor_base_test("installments")

    assert processor.endpoint_deduplication_key == "encoded_key"
    assert processor.last_bookmark_value == "2021-06-01T00:00:00Z"
