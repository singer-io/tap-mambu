from . import setup_processor_base_test


def test_communications_accounts_processor():
    processor = setup_processor_base_test("communications")

    assert processor.endpoint_id_field == "encoded_key"
