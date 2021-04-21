"""
Test tap discovery
"""
import re
from tap_tester import connections, menagerie
from base import MambuBaseTest

class DiscoveryTest(MambuBaseTest):
    """ Test the tap discovery """

    @staticmethod
    def name():
        return "tap_tester_mambu_discovery_test"

    def test_run(self):
        """
        • Verify that discover creates the appropriate catalog, schema, metadata, etc.
        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          • streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL_TABLE
        • verify the actual replication matches our expected replication method
        • verify that primary and replication keys are given the inclusion of automatic metadata
        • verify that all other fields have inclusion of available metadata
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)
        self.run_and_verify_check_mode(conn_id)

        # Verify number of actual streams discovered match expected
        catalogs = menagerie.get_catalogs(conn_id)
        found_catalogs = [catalog for catalog in catalogs
                          if catalog.get('tap_stream_id') in streams_to_test]

        self.assertGreater(len(found_catalogs),
                           0,
                           msg="unable to locate schemas for connection {}".format(conn_id))
        self.assertEqual(len(streams_to_test),
                         len(found_catalogs),
                         msg="Expected {} streams, actual was {} for connection {}".format(
                             len(streams_to_test),
                             len(found_catalogs),
                             conn_id)
                         )

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(streams_to_test),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]

                # verify the stream level properties are as expected

                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                stream_metadata = stream_properties[0].get("metadata", {})
                self.assertTrue(len(stream_properties) == 1,
                                msg=("There is NOT only one top level breadcrumb for {}"
                                "\nstream_properties | {}").format(stream, stream_properties))

                # verify replication key(s)
                expected = self.expected_replication_keys()[stream]
                actual = set(stream_metadata.get(self.REPLICATION_KEYS, []))

                self.assertEqual(
                    expected,
                    actual,
                    msg="expected replication key {} but actual is {}".format(expected, actual))

                # verify primary key(s)
                expected = self.expected_primary_keys()[stream]
                actual = set(stream_metadata.get(self.PRIMARY_KEYS, []))

                self.assertEqual(
                    expected,
                    actual,
                    msg="expected primary key {} but actual is {}".format(expected, actual))

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL


                actual_replication_method = stream_metadata.get(self.REPLICATION_METHOD)

                if stream_metadata.get(self.REPLICATION_KEYS):
                    self.assertTrue(
                        self.INCREMENTAL == actual_replication_method,
                        msg="Expected INCREMENTAL replication since there is a replication key")
                else:
                    self.assertTrue(
                        self.FULL_TABLE == actual_replication_method,
                        msg="Expected FULL_TABLE replication since there is no replication key")

                # verify the actual replication matches our expected replication method
                expected_replication_method = self.expected_replication_method().get(stream, None)
                self.assertEqual(
                    expected_replication_method,
                    actual_replication_method,
                    msg="expected replication method {} but actual is {}".format(
                        expected_replication_method,
                        actual_replication_method))

                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # verify that primary and replication keys are given `"inclusion": "automatic"`
                # metadata.

                for item in metadata:
                    # Skip the stream level metadata
                    if item.get("breadcrumb") == []:
                        continue

                    actual_field_inclusion = item.get("metadata", {}).get("inclusion")
                    field_name = item.get("breadcrumb", ["properties", None])[1]

                    if field_name:
                        if actual_field_inclusion == "automatic":
                            self.assertIn(field_name, expected_automatic_fields)
                        elif actual_field_inclusion == "available":
                            self.assertNotIn(field_name, expected_automatic_fields)
                        else:
                            raise RuntimeError("Stream {} got unexpected `inclusion` value {}"
                                               .format(stream, field_name))
                    else:
                        raise RuntimeError("Got null field name on stream {}".format(stream))
