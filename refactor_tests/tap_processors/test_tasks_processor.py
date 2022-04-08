from . import setup_processor_base_test


def test_tasks_processor():
    processor = setup_processor_base_test("tasks")

    assert processor.endpoint_deduplication_key == "id"
