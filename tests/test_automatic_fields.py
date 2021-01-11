"""
Test that when no fields are selected for a stream, automatic fields are still replicated
"""

from tap_tester import runner, menagerie, connections

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

    def verify_stream_and_field_selection(self, conn_id):
        """
        For expected sync streams, verify that
        - no fields are selected
        - automatic fields are automatic
        - non-automatic fields are "inclusion": "available"
        """

        catalogs = menagerie.get_catalogs(conn_id)

        for catalog_entry in catalogs:
            if catalog_entry["tap_stream_id"] in self.expected_sync_streams():
                schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
                entry_metadata = schema.get('metadata', [])

                for mdata in entry_metadata:
                    is_selected = mdata.get('metadata').get('selected')
                    if mdata.get('breadcrumb') == []:
                        self.assertTrue(is_selected)
                    else:
                        inclusion = mdata.get('metadata', {}).get('inclusion')
                        field_name = mdata.get('breadcrumb', ['properties', None])[1]
                        automatic_fields = self.expected_automatic_fields()[catalog_entry['tap_stream_id']]

                        self.assertIsNotNone(field_name)

                        if field_name in automatic_fields:
                            self.assertTrue(inclusion == 'automatic')
                        else:
                            self.assertFalse(is_selected)
                            self.assertTrue(inclusion == 'available')

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
        (conn_id,
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
