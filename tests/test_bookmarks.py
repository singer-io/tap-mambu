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

    def test_run(self):
        """
        Verify that we can get multiple pages of data for each stream
        """

        conn_id = self.create_connection()
        catalogs = menagerie.get_catalogs(conn_id)

        self.select_all_streams_and_fields(conn_id, catalogs)
        self.verify_stream_and_field_selection(conn_id)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)

        first_sync_bookmarks = menagerie.get_state(conn_id)
        first_sync_records = runner.get_records_from_target_output()


        new_bookmarks = {}
        for stream_name, current_bookmark in first_sync_bookmarks['bookmarks'].items():
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
        second_sync_bookmarks = menagerie.get_state(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_method().get(stream)

                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                if replication_method == self.INCREMENTAL:
                    replication_key = self.expected_replication_keys().get(stream).pop()

                    first_sync_bookmark_value = first_sync_bookmarks['bookmarks'][stream]
                    second_sync_bookmark_value = second_sync_bookmarks['bookmarks'][stream]
                    # bookmarked values as utc datetime objects for comparing against records
                    first_sync_bookmark_value_utc = singer.utils.strptime_to_utc(first_sync_bookmark_value)
                    second_sync_bookmark_value_utc = singer.utils.strptime_to_utc(second_sync_bookmark_value)

                    simulated_bookmark_value = new_state['bookmarks'][stream]

                    # Verify the both syncs end on the same bookmark
                    self.assertEqual(first_sync_bookmark_value,
                                     second_sync_bookmark_value)

                    # Verify that first sync records fall betwen the start date and the final
                    # bookmark value
                    for message in first_sync_messages:
                        lower_bound = singer.utils.strptime_to_utc(self.get_properties()['start_date'])
                        actual_value = singer.utils.strptime_to_utc(message.get('data').get(replication_key))
                        upper_bound = first_sync_bookmark_value_utc
                        self.assertTrue(lower_bound <= actual_value <= upper_bound,
                                        msg="First sync records fall outside of expected sync window")

                    # Verify the second sync records fall between simulated bookmark value and the
                    # final bookmark value
                    for message in second_sync_messages:
                        lower_bound = singer.utils.strptime_to_utc(simulated_bookmark_value)
                        actual_value = singer.utils.strptime_to_utc(message.get('data').get(replication_key))
                        upper_bound = second_sync_bookmark_value_utc
                        self.assertTrue(lower_bound <= actual_value <= upper_bound,
                                        msg="Second sync records fall outside of expected sync window")


                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                    # Verify at least 1 record was replicated in the second sync
                    self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))

                elif replication_method == self.FULL_TABLE:
                    # Verify no bookmark exists
                    self.assertNotIn(stream, first_sync_bookmarks['bookmarks'])
                    self.assertNotIn(stream, second_sync_bookmarks['bookmarks'])

                else:
                    raise NotImplementedError("invalid replication method: {}".format(replication_method))
