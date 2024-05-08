"""
Test that when no fields are selected for a stream, automatic fields are still replicated
"""

from tap_tester import connections, runner
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

        conn_id = connections.ensure_connection(self)
        self.run_and_verify_check_mode(conn_id)

        self.select_and_verify_fields(conn_id)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        all_records_by_stream = runner.get_records_from_target_output()
        page_size = int(self.get_properties()['page_size'])

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                # Assert all expected streams synced at least a full pages of records
                self.assertGreater(
                    record_count_by_stream.get(stream, 0),
                    page_size,
                    msg="{} did not sync more than a page of records".format(stream)
                )

                records = [ x['data'] for x in all_records_by_stream[stream]['messages']]

                unique_records = self.get_unique_records(stream, records)

                self.assertGreater(len(unique_records),
                                   page_size)
