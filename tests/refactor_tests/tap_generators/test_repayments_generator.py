from unittest.mock import MagicMock

import mock
import tap_mambu.tap_mambu_refactor.tap_generators.child_generator

from ..constants import config_json


def test_repayments_generator_endpoint_config_init():
    from tap_mambu.tap_mambu_refactor.tap_generators.loan_repayments_generator import LoanRepaymentsGenerator
    client_mock = MagicMock()
    client_mock.page_size = int(config_json.get("page_size", 500))
    client_mock.request = MagicMock()
    generator = LoanRepaymentsGenerator(stream_name="loan_accounts",
                                        client=client_mock,
                                        config=config_json,
                                        state={'currently_syncing': 'loan_accounts'},
                                        sub_type="self",
                                        parent_id="ASD")

    assert generator.endpoint_api_key_type is None
    assert generator.endpoint_parent_id == "ASD"
    assert generator.endpoint_api_version == "v1"
