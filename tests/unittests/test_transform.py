from tap_mambu.transform import transform_json, convert_custom_fields, remove_custom_nodes
import unittest


class TestTransformJson(unittest.TestCase):

    def test_transform_json_handles_dictionary_custom_fields(self):
        expected = [{'custom_fields': [{'field_set_id': '_custom_1', 'id': 'id', 'value': '1'}],
                     'id': '1'}]
        actual = transform_json([{"_custom_1" : {"id": '1'}, "id": '1'}],
                                "my_path")
        self.assertEqual(expected, actual)

    def test_transform_json_handles_list_custom_fields(self):
        expected = [{'custom_fields': [{'field_set_id': '_custom_1', 'id': 'id', 'value': '1'},
                                       {'field_set_id': '_custom_2', 'id': 'id', 'value': '2'},
                                       {'field_set_id': '_custom_2', 'id': 'index', 'value': '0'},
                                       {'field_set_id': '_custom_2', 'id': 'id', 'value': '3'},
                                       {'field_set_id': '_custom_2', 'id': 'index', 'value': '1'}],
                     'id': '1'}]

        actual = transform_json([{"_custom_1" : {"id": '1'},
                                  "_custom_2" : [{"id": '2', "index": '0'},
                                                 {"id": '3', "index": '1'}],
                                  "id": '1'}],
                                "my_path")

        self.assertEqual(expected, actual)

    def test_transform_no_custom_fields(self):
        expected = [{'custom_fields': [],
                     'id': '1'}]
        actual = transform_json([{"id": '1'}],
                                "my_path")
        self.assertEquals(expected, actual)
