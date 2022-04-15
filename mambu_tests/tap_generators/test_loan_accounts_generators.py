from . import setup_generator_base_test


def test_loan_accounts_generator():
    generators = setup_generator_base_test("loan_accounts")

    assert 2 == len(generators)

    lm_generator = generators[0]

    assert lm_generator.endpoint_path == "loans:search"
    assert lm_generator.endpoint_bookmark_field == "lastModifiedDate"
    assert lm_generator.endpoint_sorting_criteria == {
            "field": "id",
            "order": "ASC"
        }
    assert lm_generator.endpoint_filter_criteria == [
            {
                "field": "lastModifiedDate",
                "operator": "AFTER",
                "value": '2021-06-01'
            }
        ]

    ad_generator = generators[1]

    assert ad_generator.endpoint_path == "loans:search"
    assert ad_generator.endpoint_bookmark_field == "lastAccountAppraisalDate"
    assert ad_generator.endpoint_sorting_criteria == {
            "field": "id",
            "order": "ASC"
        }
    assert ad_generator.endpoint_filter_criteria == [
        {
            "field": "lastAccountAppraisalDate",
            "operator": "AFTER",
            "value": '2021-06-01'
        }
    ]
