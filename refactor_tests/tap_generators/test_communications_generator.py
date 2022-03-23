from . import setup_generator_base_test


def test_communications_generator_endpoint_config_init():
    generators = setup_generator_base_test("communications")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'communications/messages:search'
    assert generator.endpoint_params == {
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "creationDate"
    assert not hasattr(generator.endpoint_body, 'filterCriteria')
    assert not hasattr(generator.endpoint_body, 'sortingCriteria')
