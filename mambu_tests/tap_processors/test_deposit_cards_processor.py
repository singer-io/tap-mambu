from . import setup_processor_base_test


def test_deposit_cards_processor_endpoint_config_init():
    processor = setup_processor_base_test("cards")

    assert processor.endpoint_id_field == 'reference_token'
    assert processor.endpoint_parent == 'deposit'
    assert processor.endpoint_parent_id == 'TEST'
    assert processor.parent_replication_field == 'deposit_last_modified_date'
    assert processor.generators[0].endpoint_bookmark_field == 'deposit_last_modified_date'
