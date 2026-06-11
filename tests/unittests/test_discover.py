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

    def test_probe_uses_page_size_1(self):
        client = self._client()
        check_stream_access(client, "branches")
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get("params", {})
        self.assertEqual(params.get("pageSize"), 1)


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
    def test_all_inaccessible_raises_exception(self, mock_check):
        mock_check.return_value = False
        with self.assertRaises(Exception) as ctx:
            discover(MagicMock())
        self.assertIn("do not have read access", str(ctx.exception))

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
