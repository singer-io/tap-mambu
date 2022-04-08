import mock
from mock import PropertyMock

from tap_mambu.sync import sync_endpoint
from .helpers import IsInstanceMockMatcher


@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsLMGenerator")
@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsADGenerator")
@mock.patch("tap_mambu.helpers.generator_processor_pairs.LoanAccountsProcessor")
def test_sync_endpoint_refactor(mock_loan_accounts_processor, mock_loan_accounts_ad_generator,
                                mock_loan_accounts_lm_generator):
    type(mock_loan_accounts_lm_generator.return_value).type = PropertyMock(return_value="LoanAccountsLMGenerator")
    type(mock_loan_accounts_ad_generator.return_value).type = PropertyMock(return_value="LoanAccountsADGenerator")
    client = "client"
    catalog = "catalog"
    state = "state"
    stream_name = "loan_accounts"
    sub_type = "sub_type"
    config = "config"
    sync_endpoint(client=client, catalog=catalog, state=state, stream_name=stream_name,
                  sub_type=sub_type, config=config)
    mock_loan_accounts_lm_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_ad_generator.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                            sub_type=sub_type, config=config)
    mock_loan_accounts_processor.assert_called_once_with(client=client, state=state, stream_name=stream_name,
                                                         sub_type=sub_type, config=config, catalog=catalog,
                                                         generators=[IsInstanceMockMatcher("LoanAccountsLMGenerator"),
                                                                     IsInstanceMockMatcher("LoanAccountsADGenerator")])
