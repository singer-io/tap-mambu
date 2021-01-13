"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import connections
from base import MambuBaseTest

class SyncCanaryTest(MambuBaseTest):
    """
    Smoke test
    """

    @staticmethod
    def name():
        return "tap_tester_mambu_sync_canary_test"

    def test_run(self):
        """
        Run tap in check mode, then select all streams and all fields within streams. Run a sync and
        verify exit codes do not throw errors. This is meant to be a smoke test for the tap. If this
        is failing do not expect any other tests to pass.
        """
        conn_id = connections.ensure_connection(self)
        self.run_and_verify_check_mode(conn_id)

        self.select_and_verify_fields(conn_id)

        record_count_by_stream = self.run_and_verify_sync(conn_id)


        # Assert all expected streams synced at least one record
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                self.assertGreater(record_count_by_stream.get(stream, 0),
                                   0,
                                   msg="{} did not sync any records".format(stream))
