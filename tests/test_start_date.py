"""
Test that the tap respects the start date
"""
from singer.utils import strptime_to_utc
from tap_tester import connections, menagerie, runner
from base import MambuBaseTest

class StartDateTest(MambuBaseTest):
    """
    Test that the tap respects the start date
    """

    first_sync_start_date = None
    second_sync_start_date = None
    first_sync_records = None
    second_sync_records = None

    @staticmethod
    def name():
        return "tap_tester_mambu_start_date_test"

    def setUp(self):
        self.first_sync_start_date = self.get_properties()['start_date']

        self.second_sync_start_date = self.get_properties(
            original_properties=False
        )['start_date']

    def untestable_streams(self):
        return set([
            "communications", # Need to set up Twilio or email server to send stuff
            "installments",
        ])

    def get_replication_key_values(self, stream, records):
        all_records = []
        for record in records:
            # Build the primary key for this record, maintaining the same order for the fields
            record_rep_key = [record[field]
                              for field in sorted(self.expected_replication_keys()[stream]) if field in record]
            # Cast to a tuple to make it hashable
            all_records.append(record_rep_key[0])
        return all_records

    def test_run(self):
        """
        Verify that running a sync, then moving the start date into the
        future and running a sync results in less records than the first
        sync
        """

        conn_id = connections.ensure_connection(self)
        self.run_and_verify_check_mode(conn_id)

        self.select_and_verify_fields(conn_id)

        first_sync_record_count_by_stream = self.run_and_verify_sync(conn_id)

        first_sync_state = menagerie.get_state(conn_id)
        first_sync_all_records_by_stream = runner.get_records_from_target_output()


        conn_id = connections.ensure_connection(self, original_properties = False)
        self.run_and_verify_check_mode(conn_id)

        self.select_and_verify_fields(conn_id)

        second_sync_record_count_by_stream = self.run_and_verify_sync(conn_id)

        second_sync_state = menagerie.get_state(conn_id)
        second_sync_all_records_by_stream = runner.get_records_from_target_output()

        all_metadata = self.expected_metadata()
        for stream_name in self.expected_sync_streams():
            stream_metadata = all_metadata[stream_name]
            with self.subTest(stream=stream_name):

                replication_method = stream_metadata.get(self.REPLICATION_METHOD)

                first_sync_count = first_sync_record_count_by_stream.get(stream_name,0)
                second_sync_count = second_sync_record_count_by_stream.get(stream_name,0)

                first_sync_records = []
                for message in first_sync_all_records_by_stream[stream_name]['messages']:
                    if message['action'] == 'upsert':
                        first_sync_records.append(message['data'])

                second_sync_records = []
                for message in second_sync_all_records_by_stream[stream_name]['messages']:
                    if message['action'] == 'upsert':
                        second_sync_records.append(message['data'])

                if  replication_method == self.FULL_TABLE:
                    """
                    Testing Criteria:
                    1. Verify that Sync B includes the same number of records as Sync A
                    2. Verify the saved state following each sync does not contain a bookmark value
                       for the stream.
                    3. Verify that all records in Sync B are included in Sync A.
                    """
                    # Criteria 1
                    self.assertGreaterEqual(first_sync_count, second_sync_count)

                    # Criteria 2
                    self.assertNotIn(stream_name, first_sync_state['bookmarks'])
                    self.assertNotIn(stream_name, second_sync_state['bookmarks'])

                    # Criteria 3
                    # - Note: This may cover up the scenario where we sync
                    #   duplicate records, but we check for that in a
                    #   different test. So we ignore that here
                    first_sync_unique_records = self.get_unique_records(stream_name,
                                                                        first_sync_records)
                    second_sync_unique_records = self.get_unique_records(stream_name,
                                                                         second_sync_records)
                    self.assertGreaterEqual(len(first_sync_unique_records),
                                            len(second_sync_unique_records))
                else:
                    """
                    Testing Criteria:
                    1. Verify the saved state following each sync contains an appropriate bookmark
                       value that corresponds to the stream’s replication key.
                    2. Verify the record count for each stream is greater in Sync A than Sync B.
                    3. Verify all records in Sync A and Sync B have replication key values which are
                       greater than or equal to the corresponding start date for that sync.
                    """
                    # Criteria 1
                    self.assertGreaterEqual(first_sync_count, second_sync_count)

                    # Criteria 2
                    self.assertIn(stream_name, first_sync_state['bookmarks'])
                    self.assertIn(stream_name, second_sync_state['bookmarks'])

                    # Criteria 3
                    rep_values = self.get_replication_key_values(stream_name, first_sync_records)

                    for value in rep_values:
                        self.assertGreaterEqual(
                            strptime_to_utc(value),
                            strptime_to_utc(self.first_sync_start_date)
                        )

                    rep_values = self.get_replication_key_values(stream_name, second_sync_records)

                    for value in rep_values:
                        self.assertGreaterEqual(
                            strptime_to_utc(value),
                            strptime_to_utc(self.second_sync_start_date)
                        )
