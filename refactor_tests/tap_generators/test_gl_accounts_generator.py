from . import setup_generator_base_test


def test_groups_generator():
    generators = setup_generator_base_test("gl_accounts")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == "glaccounts"
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params["type"] == generator.sub_type
