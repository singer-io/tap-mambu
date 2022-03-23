from . import setup_processor_base_test


def test_branches_accounts_processor():
    processor = setup_processor_base_test("index_rate_sources")

    assert processor.endpoint_deduplication_key == "encoded_key"
