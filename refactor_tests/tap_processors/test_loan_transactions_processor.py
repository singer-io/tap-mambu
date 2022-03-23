from . import setup_processor_base_test


def test_loan_transactions_processor():
    processor = setup_processor_base_test("loan_transactions")

    assert processor.endpoint_id_field == "encoded_key"
