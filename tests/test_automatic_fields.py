"""
Test that when no fields are selected for a stream, automatic fields are still replicated
"""
from tap_tester import runner
from base import MambuBaseTest

class AutomaticFieldsTest(MambuBaseTest):
    """
    Test that when no fields are selected for a stream, automatic fields
    are still replicated
    """

    @staticmethod
    def name():
        return "tap_tester_mambu_automatic_fields_test"

    def untestable_streams(self):
        return set([
            "communications", # Need to set up Twilio or email server to send stuff
        ])

    def verify_is_selected(self, is_selected):
        """
        Override the default `verify_is_selected` because we only want automatic fields selected
        """
        self.assertFalse(is_selected)

    def test_run(self):
        """
        Verify that we can get multiple pages of automatic fields for each
        stream
        """

        # conn_id = self.create_connection()
        # catalogs = menagerie.get_catalogs(conn_id)

        # # Don't select any fields
        # self.select_all_streams_and_fields(conn_id, catalogs, select_all_fields=False)

        # self.verify_stream_and_field_selection(conn_id)

        # # Run a sync job using orchestrator
        # record_count_by_stream = self.run_and_verify_sync(conn_id)
        (_,
         record_count_by_stream,
         _,
         _) = self.make_connection_and_run_sync(selection_kwargs={"select_all_fields": False})

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        # Assert all expected streams synced at least a full pages of records
        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                self.assertGreater(record_count_by_stream.get(stream, 0),
                                   self.get_properties()['page_size'],
                                   msg="{} did not sync more than a page of records".format(stream))

        for stream_name, actual_fields in actual_fields_by_stream.items():
            with self.subTest(stream=stream_name):
                self.assertSetEqual(self.expected_automatic_fields()[stream_name],
                                    actual_fields)
