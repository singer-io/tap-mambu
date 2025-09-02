import unittest
from tap_mambu.helpers import transform_json


class TestTransformJson(unittest.TestCase):

    def test_transform_json_handles_dictionary_custom_fields(self):
        expected = [{'_custom_1': {'id': '1'},
                     'id': '1',
                     'custom_fields': [{'_custom_1': {'id': '1'}}]}]
        actual = transform_json([{"_custom_1" : {"id": '1'}, "id": '1'}],
                                "my_path")
        self.assertEqual(expected, actual)

    def test_transform_json_handles_list_custom_fields(self):
        # the `_` fields will be stripped by the transformer
        expected = [{'_custom_1': {'id': '1'},
                     '_custom_2': [{'id': '2', 'index': '0'},
                                   {'id': '3', 'index': '1'}],
                     'id': '1',
                     'custom_fields': [{'_custom_1': {'id': '1'}},
                                       {'_custom_2': [{'id': '2', 'index': '0'},
                                                      {'id': '3', 'index': '1'}]}
                                        ]
                     }]
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

        self.assertEqual(expected, actual)
