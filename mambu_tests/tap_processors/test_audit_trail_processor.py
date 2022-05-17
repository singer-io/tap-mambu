import mock
from mock import MagicMock
from . import setup_processor_base_test
from ..helpers import GeneratorMock
from ..constants import config_json


def test_audit_trail_processor():
    processor = setup_processor_base_test("audit_trail")

    assert processor.bookmark_offset == 1


@mock.patch("tap_mambu.helpers.write_state")
def test_audit_trail_update_bookmark(mock_write_state):
    from tap_mambu import discover
    from tap_mambu.tap_processors.audit_trail_processor import AuditTrailProcessor

    catalog = discover()
    client_mock = MagicMock()
    generator_mock = GeneratorMock([])
    generator_mock.static_params = {}
    processor = AuditTrailProcessor(catalog=catalog,
                                    stream_name="audit_trail",
                                    client=client_mock,
                                    config=config_json,
                                    state={'currently_syncing': 'audit_trail'},
                                    sub_type="self",
                                    generators=[generator_mock])

    assert processor.bookmark_offset == 1
    processor._update_bookmark({"occurred_at": "2021-10-01T00:00:00.000000Z"}, "occurred_at")
    assert processor.bookmark_offset == 1
    processor._update_bookmark({"occurred_at": "2021-10-01T00:00:00.000000Z"}, "occurred_at")
    assert processor.bookmark_offset == 2
    processor._update_bookmark({"occurred_at": "2021-10-01T00:00:00.000000Z"}, "occurred_at")
    assert processor.bookmark_offset == 3
    processor.write_bookmark()

    expected_state = {'currently_syncing': 'audit_trail', 'bookmarks': {
        'audit_trail': ['2021-10-01T00:00:00.000000Z', 3]}}
    mock_write_state.assert_called_once_with(expected_state)


@mock.patch("tap_mambu.helpers.write_state")
def test_audit_trail_write_bookmark(mock_write_state):
    from tap_mambu import discover
    from tap_mambu.tap_processors.audit_trail_processor import AuditTrailProcessor

    catalog = discover()
    client_mock = MagicMock()
    processor = AuditTrailProcessor(catalog=catalog,
                                    stream_name="audit_trail",
                                    client=client_mock,
                                    config=config_json,
                                    state={'currently_syncing': 'audit_trail'},
                                    sub_type="self",
                                    generators=[GeneratorMock([])])

    processor.write_bookmark()

    expected_state = {'currently_syncing': 'audit_trail', 'bookmarks': {
        'audit_trail': ['2021-06-01T00:00:00Z', 1]}}
    mock_write_state.assert_called_once_with(expected_state)
