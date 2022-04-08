from . import setup_processor_base_test


def test_users_processor():
    processor = setup_processor_base_test("users")

    assert processor.endpoint_deduplication_key == "id"
