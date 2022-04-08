from . import setup_generator_base_test


def test_custom_field_sets_generator_endpoint_config_init():
    generators = setup_generator_base_test("custom_field_sets")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == "customfieldsets"
    assert generator.endpoint_api_method == "GET"
