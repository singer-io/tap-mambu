from . import setup_processor_base_test


def test_interest_accrual_breakdown_processor():
    processor = setup_processor_base_test("interest_accrual_breakdown")

    assert processor.endpoint_deduplication_key == "entry_id"
