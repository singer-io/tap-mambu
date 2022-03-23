from . import setup_generator_base_test


def test_tasks_generator_endpoint_config_init():
    generators = setup_generator_base_test("tasks")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'tasks'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
        "sortBy": "lastModifiedDate:ASC",
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
