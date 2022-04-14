from . import setup_processor_base_test


def test_loan_repayments_processor_endpoint_config_init():
    processor = setup_processor_base_test("loan_repayments")

    assert processor.endpoint_parent == 'loan_accounts'
    assert processor.endpoint_parent_id == 'TEST'
