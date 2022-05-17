from . import setup_generator_base_test


def test_loan_transactions_generator_endpoint_config_init():
    generators = setup_generator_base_test("loan_transactions")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'loans/transactions:search'
    assert generator.endpoint_sorting_criteria == {
        "field": "creationDate",
        "order": "ASC"
    }
    assert generator.endpoint_bookmark_field == "creationDate"
    assert generator.endpoint_filter_criteria == [
        {
            "field": "creationDate",
            "operator": "AFTER",
            "value": '2021-06-01'
        }
    ]
