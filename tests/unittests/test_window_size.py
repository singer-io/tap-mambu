from parameterized import parameterized
import mock

from tap_mambu.helpers.client import MambuClient
from tap_mambu.helpers.constants import DEFAULT_DATE_WINDOW_SIZE, DEFAULT_PAGE_SIZE

config = {
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "apikey": "YOUR_APIKEY",
    "subdomain": "YOUR_SUBDOMAIN",
    "start_date": "2019-01-01T00:00:00Z",
    "lookback_window": 30,
    "user_agent": "tap-mambu <api_user_email@your_company.com>",
    "page_size": "500",
    "apikey_audit": "AUDIT_TRAIL_APIKEY"}


@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
class TestGetWindowSize():
    def test_non_value(self, mock_check_access):
        """
        Test if no window size is not passed in the config, then set it to the default value.
        """
        # Verify that the default window size value is set.
        with MambuClient(config.get('username'),
                     config.get('password'),
                     config.get('apikey'),
                     config['subdomain'],
                     config.get('apikey_audit'),
                     int(config.get('page_size', DEFAULT_PAGE_SIZE)),
                     user_agent=config['user_agent'],
                     window_size=config.get('window_size')) as client:
            # Verify window size value is expected
            assert client.window_size == DEFAULT_DATE_WINDOW_SIZE

    @parameterized.expand([
        ["None_value", None, DEFAULT_DATE_WINDOW_SIZE],
        ["integer_value", 10, 10],
        ["float_value", 100.5, 100],
        ["string_integer", "10", 10],
        ["string_float", "100.5", 100],
    ])
    def test_window_size_values(self, mock_check_access, name, date_window_size, expected_value):
        """
        Test that for the valid value of window size,
        No exception is raised and the expected value is set.
        """
        with MambuClient(config.get('username'),
                     config.get('password'),
                     config.get('apikey'),
                     config['subdomain'],
                     config.get('apikey_audit'),
                     int(config.get('page_size', DEFAULT_PAGE_SIZE)),
                     user_agent=config['user_agent'],
                     window_size=date_window_size) as client:
            # Verify window size value is expected
            assert client.window_size == expected_value

    @parameterized.expand([
        ["zero_string", "0"],
        ["less_than_1_string", "0.5"],
        ["negative_value", -10],
        ["string_negative_value", "-100"],
        ["string_alphabate", "abc"],
    ])
    def test_invalid_value(self, mock_check_access, name, date_window_size):
        """
        Test that for invalid value exception is raised.
        """
        actual_exc_string = ""
        expected_exc_string = "The entered window size '{}' is invalid; it should be a valid non-zero integer.".format(date_window_size)
        try:
            MambuClient(config.get('username'),
                     config.get('password'),
                     config.get('apikey'),
                     config['subdomain'],
                     config.get('apikey_audit'),
                     int(config.get('page_size', DEFAULT_PAGE_SIZE)),
                     user_agent=config['user_agent'],
                     window_size=date_window_size)
        except Exception as e:
            # Verify that the exception message is expected.
            actual_exc_string = str(e)

        assert actual_exc_string == expected_exc_string
