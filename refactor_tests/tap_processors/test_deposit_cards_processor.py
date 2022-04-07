from . import setup_processor_base_test


def test_deposit_cards_processor_endpoint_config_init():
    processor = setup_processor_base_test("cards")

    assert processor.endpoint_id_field == 'reference_token'
    assert processor.endpoint_parent == 'deposit'
    assert processor.endpoint_parent_id == 'TEST'
