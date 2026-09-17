from . import setup_processor_base_test


def test_loan_repayments_processor_endpoint_config_init():
    processor = setup_processor_base_test("loan_repayments")

    assert processor.endpoint_parent == 'loan_accounts'
    assert processor.endpoint_parent_id == 'TEST'
    assert processor.parent_replication_field == 'loan_accounts_last_modified_date'
    assert processor.generators[0].endpoint_bookmark_field == 'loan_accounts_last_modified_date'
