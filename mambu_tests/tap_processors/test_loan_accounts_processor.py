from . import setup_processor_base_test


def test_loan_accounts_processor():
    processor = setup_processor_base_test("loan_accounts")

    assert processor.endpoint_deduplication_key == "id"
    assert processor.endpoint_child_streams == ["loan_repayments"]
