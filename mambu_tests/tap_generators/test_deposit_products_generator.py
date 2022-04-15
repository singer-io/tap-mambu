from . import setup_generator_base_test


def test_deposit_products_generator_endpoint_config_init():
    generators = setup_generator_base_test("deposit_products")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == "depositproducts"
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
