import unittest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_mambu.helpers.discover import discover, check_stream_access
from tap_mambu.helpers.client import (
    MambuUnauthorizedError,
    MambuForbiddenError,
    MambuNotFoundError,
    MambuMethodNotAllowedError,
    MambuNoAuditApikeyInConfig,
    MambuBadRequestError,
)


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):

    def _client(self):
        return MagicMock()

    def test_returns_true_when_accessible(self):
        client = self._client()
        result = check_stream_access(client, "branches")
        self.assertTrue(result)
        client.request.assert_called_once()

    def test_returns_false_on_unauthorized(self):
        client = self._client()
        client.request.side_effect = MambuUnauthorizedError("401")
        result = check_stream_access(client, "branches")
        self.assertFalse(result)

    def test_returns_false_on_forbidden(self):
        client = self._client()
        client.request.side_effect = MambuForbiddenError("403")
        result = check_stream_access(client, "users")
        self.assertFalse(result)

    def test_returns_false_on_missing_audit_apikey(self):
        client = self._client()
        client.request.side_effect = MambuNoAuditApikeyInConfig("missing audit key")
        result = check_stream_access(client, "audit_trail")
        self.assertFalse(result)

    def test_returns_false_on_not_found(self):
        """404 means the endpoint doesn't exist — stream will fail at runtime."""
        client = self._client()
        client.request.side_effect = MambuNotFoundError("404")
        result = check_stream_access(client, "branches")
        self.assertFalse(result)

    def test_returns_false_on_method_not_allowed(self):
        """405 means the HTTP method is not supported — stream will fail at runtime."""
        client = self._client()
        client.request.side_effect = MambuMethodNotAllowedError("405")
        result = check_stream_access(client, "branches")
        self.assertFalse(result)

    def test_reraises_non_mambu_errors(self):
        client = self._client()
        client.request.side_effect = ConnectionError("network timeout")
        with self.assertRaises(ConnectionError):
            check_stream_access(client, "branches")

    def test_get_stream_uses_get_method(self):
        client = self._client()
        check_stream_access(client, "branches")
        call_kwargs = client.request.call_args
        self.assertEqual(call_kwargs.kwargs["method"], "GET")

    def test_post_stream_uses_post_method(self):
        client = self._client()
        check_stream_access(client, "clients")
        call_kwargs = client.request.call_args
        self.assertEqual(call_kwargs.kwargs["method"], "POST")

    def test_clients_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "clients")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "lastModifiedDate")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)

    def test_deposit_accounts_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "deposit_accounts")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "lastModifiedDate")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)

    def test_post_probe_uses_offset_limit_params(self):
        client = self._client()
        check_stream_access(client, "clients")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("offset"), 0)
        self.assertEqual(params.get("limit"), 1)

    def test_audit_trail_uses_audit_apikey_type(self):
        client = self._client()
        check_stream_access(client, "audit_trail")
        call_kwargs = client.request.call_args
        self.assertEqual(call_kwargs.kwargs.get("apikey_type"), "audit")

    def test_activities_uses_v1(self):
        client = self._client()
        check_stream_access(client, "activities")
        call_kwargs = client.request.call_args
        self.assertEqual(call_kwargs.kwargs.get("version"), "v1")

    def test_probe_uses_page_size_1_for_default_probe_streams(self):
        client = self._client()
        check_stream_access(client, "branches")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("pageSize"), 1)

    def test_activities_probe_uses_date_window_params(self):
        client = self._client()
        check_stream_access(client, "activities")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("from"), "1970-01-01")
        self.assertEqual(params.get("to"), "1970-01-01")

    def test_audit_trail_probe_uses_occurred_at_range(self):
        client = self._client()
        check_stream_access(client, "audit_trail")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("sort_order"), "asc")
        self.assertEqual(params.get("occurred_at[gte]"), "1970-01-01T00:00:00Z")
        self.assertEqual(params.get("occurred_at[lte]"), "1970-01-01T00:00:00Z")

    def test_installments_probe_uses_due_date_range(self):
        client = self._client()
        check_stream_access(client, "installments")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("dueFrom"), "1970-01-01")
        self.assertEqual(params.get("dueTo"), "1970-01-01")

    def test_credit_arrangements_probe_uses_creation_date_sort(self):
        client = self._client()
        check_stream_access(client, "credit_arrangements")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("sortBy"), "creationDate:ASC")

    def test_loan_products_probe_uses_id_sort(self):
        client = self._client()
        check_stream_access(client, "loan_products")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("sortBy"), "id:ASC")

    def test_tasks_probe_uses_last_modified_date_sort(self):
        client = self._client()
        check_stream_access(client, "tasks")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("sortBy"), "lastModifiedDate:ASC")

    def test_users_probe_uses_id_sort(self):
        client = self._client()
        check_stream_access(client, "users")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("sortBy"), "id:ASC")

    def test_deposit_products_probe_uses_offset_limit_params(self):
        client = self._client()
        check_stream_access(client, "deposit_products")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("offset"), 0)
        self.assertEqual(params.get("limit"), 1)

    def test_deposit_transactions_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "deposit_transactions")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "id")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)
        self.assertEqual(body.get("filterCriteria", [])[0].get("field"), "creationDate")

    def test_gl_accounts_probe_uses_type_param(self):
        client = self._client()
        check_stream_access(client, "gl_accounts")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("type"), "ASSET")

    def test_gl_journal_entries_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "gl_journal_entries")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "entryId")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)
        self.assertEqual(body.get("filterCriteria", [])[0].get("field"), "creationDate")

    def test_groups_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "groups")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "lastModifiedDate")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)

    def test_index_rate_sources_probe_uses_no_params(self):
        client = self._client()
        check_stream_access(client, "index_rate_sources")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", None)
        self.assertEqual(params, {})

    def test_interest_accrual_breakdown_probe_uses_date_filter_body(self):
        client = self._client()
        check_stream_access(client, "interest_accrual_breakdown")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "entryId")
        self.assertEqual(body.get("filterCriteria", [])[0].get("value"), "1970-01-01")

    def test_loan_accounts_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "loan_accounts")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "id")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)

    def test_loan_transactions_probe_uses_search_filter_body(self):
        client = self._client()
        check_stream_access(client, "loan_transactions")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json", {})
        self.assertEqual(body.get("sortingCriteria", {}).get("field"), "id")
        self.assertEqual(len(body.get("filterCriteria", [])), 2)
        self.assertEqual(body.get("filterCriteria", [])[0].get("field"), "creationDate")

    def test_communications_probe_uses_filter_body(self):
        client = self._client()
        check_stream_access(client, "communications")
        call_kwargs = client.request.call_args
        body = call_kwargs.kwargs.get("json")
        self.assertIsInstance(body, list)
        self.assertEqual(len(body), 3)
        self.assertEqual(body[0].get("field"), "creationDate")
        self.assertEqual(body[0].get("operator"), "AFTER")
        self.assertEqual(body[2].get("field"), "state")
        self.assertEqual(body[2].get("value"), "SENT")


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_discover_returns_catalog(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertIsInstance(catalog, Catalog)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_catalog_contains_all_streams_when_all_accessible(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        from tap_mambu.helpers.schema import STREAMS
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_inaccessible_stream_excluded(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "users"
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("users", stream_ids)
        self.assertIn("branches", stream_ids)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_child_stream_excluded_when_parent_inaccessible(self, mock_check):
        """'cards' is excluded when 'deposit_accounts' is inaccessible."""
        mock_check.side_effect = lambda client, name: name != "deposit_accounts"
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("deposit_accounts", stream_ids)
        self.assertNotIn("cards", stream_ids)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_child_stream_included_when_parent_accessible(self, mock_check):
        """'cards' is included when 'deposit_accounts' is accessible."""
        mock_check.return_value = True
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertIn("deposit_accounts", stream_ids)
        self.assertIn("cards", stream_ids)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_loan_repayments_excluded_when_loan_accounts_inaccessible(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "loan_accounts"
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("loan_accounts", stream_ids)
        self.assertNotIn("loan_repayments", stream_ids)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_child_streams_not_probed_directly(self, mock_check):
        """check_stream_access must never be called for child streams."""
        mock_check.return_value = True
        discover(MagicMock())
        probed = {call.args[1] for call in mock_check.call_args_list}
        self.assertNotIn("cards", probed)
        self.assertNotIn("loan_repayments", probed)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_warning_logged_for_excluded_stream(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "users"
        with patch("tap_mambu.helpers.discover.LOGGER") as mock_logger:
            discover(MagicMock())
        warned = [call.args[1] for call in mock_logger.warning.call_args_list]
        self.assertIn("users", warned)

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_consolidated_warning_includes_pruned_child_streams(self, mock_check):
        mock_check.side_effect = lambda client, name: name != "deposit_accounts"
        with patch("tap_mambu.helpers.discover.LOGGER") as mock_logger:
            discover(MagicMock())

        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        self.assertTrue(any("No 'read' access to stream(s):" in call for call in warning_calls))
        self.assertTrue(any("deposit_accounts, cards" in call for call in warning_calls))

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_all_inaccessible_raises_exception(self, mock_check):
        mock_check.return_value = False
        with self.assertRaises(MambuForbiddenError) as ctx:
            discover(MagicMock())
        self.assertIn("do not have 'read' access to any", str(ctx.exception))

    @patch("tap_mambu.helpers.discover.check_stream_access")
    def test_incremental_stream_sets_replication_key_automatic(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        branches = next(s for s in catalog.streams if s.tap_stream_id == "branches")
        # Find the replication key field in metadata and verify inclusion=automatic
        from singer import metadata as singer_metadata
        mdata = singer_metadata.to_map(branches.metadata)
        inclusion = mdata.get(("properties", "last_modified_date"), {}).get("inclusion")
        self.assertEqual(inclusion, "automatic")
