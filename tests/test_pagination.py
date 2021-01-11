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

        conn_id = self.create_connection()
        catalogs = menagerie.get_catalogs(conn_id)

        # Select all fields
        for catalog_entry in catalogs:
            if catalog_entry["tap_stream_id"] in self.expected_sync_streams():
                schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
                connections.select_catalog_and_fields_via_metadata(
                    conn_id,
                    catalog_entry,
                    schema,
                )

        # For expected sync streams, verify that
        # - all fields are selected
        # - automatic fields are automatic
        # - non-automatic fields are "inclusion": "available"
        catalogs = menagerie.get_catalogs(conn_id)

        for catalog_entry in catalogs:
            tap_stream_id = catalog_entry['tap_stream_id']
            if tap_stream_id in self.expected_sync_streams():
                schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
                entry_metadata = schema.get('metadata', [])

                for mdata in entry_metadata:
                    is_selected = mdata.get('metadata').get('selected')
                    if mdata.get('breadcrumb') == []:
                        self.assertTrue(is_selected)
                    else:
                        inclusion = mdata.get('metadata', {}).get('inclusion')
                        field_name = mdata.get('breadcrumb', ['properties', None])[1]

                        automatic_fields = self.expected_automatic_fields()[tap_stream_id]

                        self.assertIsNotNone(field_name)

                        if field_name in automatic_fields:
                            self.assertTrue(inclusion == 'automatic')
                        else:
                            self.assertTrue(is_selected)
                            self.assertTrue(inclusion == 'available')

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        all_records_by_stream = runner.get_records_from_target_output()


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
