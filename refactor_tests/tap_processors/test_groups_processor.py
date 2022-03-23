from . import setup_processor_base_test


def test_groups_processor():
    processor = setup_processor_base_test("groups")

    assert processor.endpoint_deduplication_key == "id"
