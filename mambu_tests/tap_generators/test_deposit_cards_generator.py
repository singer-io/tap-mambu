from . import setup_generator_base_test


def test_deposit_cards_generator_endpoint_config_init():
    generators = setup_generator_base_test("cards")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_parent_id == "TEST"
    assert generator.endpoint_path == 'deposits/TEST/cards'
    assert generator.endpoint_api_method == "GET"
