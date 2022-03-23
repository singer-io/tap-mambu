from . import setup_processor_base_test


def test_centres_processor():
    processor = setup_processor_base_test("centres")

    assert processor.endpoint_deduplication_key == "id"
