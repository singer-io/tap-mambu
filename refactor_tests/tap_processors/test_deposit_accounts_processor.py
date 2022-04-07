from . import setup_processor_base_test


def test_deposit_accounts_processor():
    processor = setup_processor_base_test("deposit_accounts")

    assert processor.endpoint_child_streams == ["cards"]
