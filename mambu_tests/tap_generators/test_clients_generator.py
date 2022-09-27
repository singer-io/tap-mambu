from . import setup_generator_base_test


def test_clients_generator():
    generators = setup_generator_base_test("clients")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == "clients:search"
    assert generator.endpoint_bookmark_field == "lastModifiedDate"
    assert generator.endpoint_sorting_criteria == {
            "field": "lastModifiedDate",
            "order": "ASC"
        }
    assert generator.endpoint_filter_criteria == [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": '2021-05-31T17:00:00.000000Z'
            }
        ]
