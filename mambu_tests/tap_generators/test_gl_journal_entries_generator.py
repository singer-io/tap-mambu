from datetime import datetime

from singer import utils

from . import setup_generator_base_test


def test_gl_journal_entries_generator_endpoint_config_init():
    generators = setup_generator_base_test("gl_journal_entries")

    assert 1 == len(generators)

    generator = generators[0]

    assert generator.endpoint_path == 'gljournalentries:search'
    assert generator.endpoint_api_method == "POST"
    assert generator.endpoint_params == {
        "detailsLevel": "FULL",
        "paginationDetails": "OFF"
    }
    assert generator.endpoint_sorting_criteria == {
            "field": "entryId",
            "order": "ASC"
        }
    assert generator.endpoint_filter_criteria[0] == {
        "field": "creationDate",
        "operator": "BETWEEN",
        "value": "2021-06-01T00:00:00.000000Z",
        "secondValue": datetime.strptime(generator.endpoint_filter_criteria[0]["secondValue"],
                                         "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    }
    assert generator.endpoint_bookmark_field == "creationDate"
