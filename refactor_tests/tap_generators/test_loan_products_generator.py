from . import setup_generator_base_test


def test_loan_products_generator_endpoint_config_init():
    generators = setup_generator_base_test("loan_products")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == "loanproducts"
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params.get("sortBy") == "id:ASC"
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
