from . import setup_generator_base_test


def test_repayments_generator_endpoint_config_init():
    generators = setup_generator_base_test("loan_repayments")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_api_key_type is None
    assert generator.endpoint_parent_id == "TEST"
    assert generator.endpoint_api_version == "v1"
