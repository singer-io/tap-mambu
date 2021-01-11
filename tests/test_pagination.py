"""
Test that when no fields are selected for a stream, automatic fields are still replicated
"""

from tap_tester import runner, menagerie, connections

from base import MambuBaseTest

class PaginationTest(MambuBaseTest):
    """
    Test that the tap can paginate API endpoints
    """

    @staticmethod
    def name():
        return "tap_tester_mambu_pagination_test"

    def untestable_streams(self):
        return set([
            "communications", # Need to set up Twilio or email server to send stuff
        ])

    def test_run(self):
        """
        Verify that we can get multiple pages of unique records for each
        stream
        """

        # conn_id = self.create_connection()
        # catalogs = menagerie.get_catalogs(conn_id)

        # self.select_all_streams_and_fields(conn_id, catalogs)
        # self.verify_stream_and_field_selection(conn_id)

        # # Run a sync job using orchestrator
        # record_count_by_stream = self.run_and_verify_sync(conn_id)
        # all_records_by_stream = runner.get_records_from_target_output()

        (conn_id,
         record_count_by_stream,
         _,
         all_records_by_stream) = self.make_connection_and_run_sync()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                # Assert all expected streams synced at least a full pages of records
                self.assertGreater(
                    record_count_by_stream.get(stream, 0),
                    self.get_properties()['page_size'],
                    msg="{} did not sync more than a page of records".format(stream)
                )

                # Assert that records are unique
                records = self.filter_output_file_for_records(
                    all_records_by_stream,
                    stream
                )

                unique_records = self.get_unique_records(stream, records)

                self.assertGreater(len(unique_records),
                                   self.get_properties()['page_size'])
