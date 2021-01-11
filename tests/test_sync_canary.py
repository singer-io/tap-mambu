"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie

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
        # conn_id = self.create_connection()
        # catalogs = menagerie.get_catalogs(conn_id)

        # self.select_all_streams_and_fields(conn_id, catalogs)
        # self.verify_stream_and_field_selection(conn_id)

        # # Run a sync job using orchestrator
        # record_count_by_stream = self.run_and_verify_sync(conn_id)

        (conn_id,
         record_count_by_stream,
         _,
         _) = self.make_connection_and_run_sync()

        # Assert all expected streams synced at least one record
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                self.assertGreater(record_count_by_stream.get(stream, 0),
                                   0,
                                   msg="{} did not sync any records".format(stream))
