from tap_tester import runner, menagerie, connections

from base import MambuBaseTest
import singer
from datetime import timedelta

class BookmarksTest(MambuBaseTest):
    """
    Test that the tap can replicate multiple pages of data
    """

    @staticmethod
    def name():
        return "tap_tester_mambu_bookmarks_test"

    def untestable_streams(self):
        return set([
            "communications", # Need to set up Twilio or email server to send stuff
            "installments",
            "gl_accounts",
        ])

    def subtract_day(self, bookmark):
        bookmark_dt = singer.utils.strptime_to_utc(bookmark)
        adjusted_bookmark = bookmark_dt - timedelta(days=1)
        return singer.utils.strftime(adjusted_bookmark)

    def get_bookmark_key(self, stream):
        stuff = {
            "branches": "last_modified_date",
            "communications": "creation_date",
            "centres": "last_modified_date",
            "clients": "last_modified_date",
            "credit_arrangements": "last_modified_date",
            "deposit_accounts": "last_modified_date",
            "deposit_products": "last_modified_date",
            "deposit_transactions": "creation_date",
            "groups":"last_modified_date",
            "loan_accounts": "last_modified_date",
            "loan_products": "last_modified_date",
            "loan_transactions": "creation_date",
            "tasks": "last_modified_date",
            "users": "last_modified_date",
            "gl_journal_entries": "booking_date",
            "activities": "timestamp",
            "installments": "last_paid_date",
        }
        return stuff[stream]

    def test_run(self):
        """
        Verify that we can get multiple pages of data for each stream
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
        first_sync_record_count = self.run_and_verify_sync(conn_id)

        first_sync_state = menagerie.get_state(conn_id)
        first_sync_bookmarks = dict(first_sync_state)
        first_sync_records = runner.get_records_from_target_output()


        new_bookmarks = {}
        for stream_name, current_bookmark in first_sync_state['bookmarks'].items():
            if stream_name == 'gl_accounts':
                new_gl_bookmarks = {
                    sub_stream : self.subtract_day(sub_bookmark)
                    for sub_stream, sub_bookmark in current_bookmark.items()
                }
                new_bookmarks[stream_name] = new_gl_bookmarks
            else:
                new_bookmarks[stream_name] = self.subtract_day(current_bookmark)

        new_state = {"bookmarks" : new_bookmarks}

        menagerie.set_state(conn_id, new_state)

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_state = menagerie.get_state(conn_id)
        second_sync_bookmarks = dict(second_sync_state)
        second_sync_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_method().get(stream)

                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                if replication_method == self.INCREMENTAL:
                    replication_key = self.expected_replication_keys().get(stream).pop()
                    bookmark_key = self.get_bookmark_key(stream)  # BUG_SRCE-4609

                    # bookmarked states (actual values)
                    first_sync_bookmark_value = first_sync_bookmarks['bookmarks'][stream]
                    second_sync_bookmark_value = second_sync_bookmarks['bookmarks'][stream]
                    # bookmarked values as utc for comparing against records
                    first_sync_bookmark_value_utc = singer.utils.strptime_to_utc(first_sync_bookmark_value)
                    second_sync_bookmark_value_utc = singer.utils.strptime_to_utc(second_sync_bookmark_value)

                    simulated_bookmark_value = new_state['bookmarks'][stream]

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_sync_bookmark_value, first_sync_bookmark_value) # assumes no changes to data during test

                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    for message in first_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)
                        self.assertLessEqual(singer.utils.strptime_to_utc(replication_key_value),
                                             first_sync_bookmark_value_utc,
                                             msg="First sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                    for message in second_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)

                        # Verify the second sync records respect the previous (simulated) bookmark value
                        self.assertGreaterEqual(singer.utils.strptime_to_utc(replication_key_value),
                                                singer.utils.strptime_to_utc(simulated_bookmark_value),
                                                msg="Second sync records do not repect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(singer.utils.strptime_to_utc(replication_key_value),
                                             second_sync_bookmark_value_utc,
                                             msg="Second sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                    # Verify at least 1 record was replicated in the second sync
                    self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))

                elif replication_method == self.FULL_TABLE:
                    # Verify the first sync sets a bookmark of the expected form
                    self.assertNotIn(stream, first_sync_bookmarks['bookmarks'])

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertNotIn(stream, second_sync_bookmarks['bookmarks'])

                else:
                    raise NotImplementedError("invalid replication method: {}".format(replication_method))
