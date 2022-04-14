from singer import utils
from . import setup_generator_base_test


def test_installments_generator_endpoint_config_init():
    generators = setup_generator_base_test("installments")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'installments'
    assert generator.endpoint_api_method == "GET"
    assert generator.endpoint_params == {
        "dueFrom": "2021-06-01",
        "dueTo": utils.now().strftime("%Y-%m-%d"),
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_bookmark_field == "lastPaidDate"
