from . import setup_generator_base_test


def test_branches_generator_endpoint_config_init():
    generators = setup_generator_base_test("branches")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'branches'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
        "sortBy": "lastModifiedDate:ASC",
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
