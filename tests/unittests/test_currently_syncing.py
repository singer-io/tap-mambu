import mock
from tap_mambu.sync import sync_all_streams
from tap_mambu.helpers.client import MambuClient
from tap_mambu.helpers.constants import DEFAULT_DATE_WINDOW_SIZE, DEFAULT_PAGE_SIZE
from tap_mambu.helpers.generator_processor_pairs import get_generator_processor_for_stream


config = {"username": "YOUR_USERNAME",
          "password": "YOUR_PASSWORD",
          "apikey": "YOUR_APIKEY",
          "subdomain": "YOUR_SUBDOMAIN",
          "start_date": "2019-01-01T00:00:00Z",
          "lookback_window": 30,
          "user_agent": "tap-mambu <api_user_email@your_company.com>",
          "page_size": "500",
          "apikey_audit": "AUDIT_TRAIL_APIKEY"}


@mock.patch("tap_mambu.sync.get_stream_subtypes")
@mock.patch("tap_mambu.sync.get_generator_processor_for_stream")
@mock.patch("tap_mambu.sync.get_selected_streams")
@mock.patch("tap_mambu.sync.get_timezone_info")
@mock.patch("tap_mambu.helpers.client.MambuClient.check_access")
class TestCurrentlySyncingBookmark:
    def test_remove_delected_stream(self,
                                    mock_check_access,
                                    mock_get_timezone_info,
                                    mock_get_selected_streams,
                                    mock_get_generator_processor_for_stream,
                                    mock_get_stream_subtypes):

        client = MambuClient(config.get('username'),
                             config.get('password'),
                             config.get('apikey'),
                             config['subdomain'],
                             config.get('apikey_audit'),
                             int(config.get('page_size', DEFAULT_PAGE_SIZE)),
                             user_agent=config['user_agent'],
                             window_size=config.get('window_size'))
        catalog = {}
        state = {"bookmarks": {"stream1": "bookmark_stream_1"}, "currently_syncing": "stream2"}

        # Define the selected streams and the last stream
        selected_streams = ['stream1', 'stream3']

        # Mock the return values of the functions
        mock_get_timezone_info.return_value = None
        mock_get_generator_processor_for_stream.return_value = get_generator_processor_for_stream("tasks")
        mock_get_selected_streams.return_value = selected_streams
        mock_get_stream_subtypes.return_value = []

        # Call the sync
        sync_all_streams(client, config, catalog, state)

        assert state == {"bookmarks": {"stream1": "bookmark_stream_1"}}
