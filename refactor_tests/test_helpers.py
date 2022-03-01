import inspect
import os

import mock
from mock import MagicMock


FIXTURES_PATH = f"{os.path.dirname(os.path.abspath(inspect.stack()[0][1]))}/Fixtures"

def test_get_selected_streams():
    from singer.catalog import Catalog
    from tap_mambu.tap_mambu_refactor.helpers import get_selected_streams
    catalog = Catalog.load(f"{FIXTURES_PATH}/catalog.json")
    selected_streams = get_selected_streams(catalog)
    expected_streams = ["loan_accounts", "loan_repayments", "audit_trail"]
    assert len(selected_streams) == len(expected_streams) and set(selected_streams) == set(expected_streams)


def test_get_bookmark():
    from tap_mambu.tap_mambu_refactor.helpers import get_bookmark
    state = {"currently_syncing": "loan_accounts",
             "bookmarks": {"loan_accounts": "2021-10-01T00:00:00Z"}}
    assert get_bookmark(state, "loan_accounts", "self", "2021-06-01T00:00:00Z") == "2021-10-01T00:00:00Z"

    state = {"currently_syncing": "loan_accounts",
             "bookmarks": {"loan_accounts": "2021-10-01T00:00:00Z"}}
    assert get_bookmark(state, "deposit_accounts", "self", "2021-06-01T00:00:00Z") == "2021-06-01T00:00:00Z"

    state = {"currently_syncing": "loan_accounts"}
    assert get_bookmark(state, "loan_accounts", "self", "2021-06-01T00:00:00Z") == "2021-06-01T00:00:00Z"


def test_get_bookmark_sub_type():
    from tap_mambu.tap_mambu_refactor.helpers import get_bookmark
    state = {"currently_syncing": "loan_accounts",
             "bookmarks": {"loan_accounts": {
                 "1": "2021-10-01T00:00:00Z"
             }}}
    assert get_bookmark(state, "loan_accounts", "1", "2021-06-01T00:00:00Z") == "2021-10-01T00:00:00Z"


@mock.patch("tap_mambu.tap_mambu_refactor.helpers.write_state")
def test_write_bookmark(mock_write_state):
    from tap_mambu.tap_mambu_refactor.helpers import write_bookmark

    state = {"currently_syncing": "loan_accounts"}
    write_bookmark(state, "loan_accounts", "self", "2021-10-01T00:00:00Z")

    mock_write_state.assert_called_with({"currently_syncing": "loan_accounts",
                                         "bookmarks": {"loan_accounts": "2021-10-01T00:00:00Z"}})

    state = {"currently_syncing": "loan_accounts",
             "bookmarks": {"loan_accounts": "2021-08-01T00:00:00Z"}}
    write_bookmark(state, "loan_accounts", "self", "2021-11-01T00:00:00Z")

    mock_write_state.assert_called_with({"currently_syncing": "loan_accounts",
                                         "bookmarks": {"loan_accounts": "2021-11-01T00:00:00Z"}})


@mock.patch("tap_mambu.tap_mambu_refactor.helpers.write_state")
def test_write_bookmark_sub_type(mock_write_state):
    from tap_mambu.tap_mambu_refactor.helpers import write_bookmark

    state = {"currently_syncing": "loan_accounts"}
    write_bookmark(state, "loan_accounts", "1", "2021-10-01T00:00:00Z")

    mock_write_state.assert_called_with({"currently_syncing": "loan_accounts",
                                         "bookmarks": {"loan_accounts": {
                                             "1": "2021-10-01T00:00:00Z"
                                         }}})

    state = {"currently_syncing": "loan_accounts",
             "bookmarks": {"loan_accounts": {
                 "1": "2021-08-01T00:00:00Z"
             }}}
    write_bookmark(state, "loan_accounts", "1", "2021-11-01T00:00:00Z")

    mock_write_state.assert_called_with({"currently_syncing": "loan_accounts",
                                         "bookmarks": {"loan_accounts": {
                                             "1": "2021-11-01T00:00:00Z"
                                         }}})
