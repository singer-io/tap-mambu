import time
import requests

from typing import List
from .. import TapGenerator


class LoanAccountsGenerator(TapGenerator):
    def __init__(self, stream_name, client, config, endpoint_config):
        super(LoanAccountsGenerator, self).__init__(stream_name, client, config, endpoint_config)

    def __iter__(self):
        return super(LoanAccountsGenerator, self).__iter__()

    def __next__(self):
        return super(LoanAccountsGenerator, self).__next__()

    def fetch_batch(self):
        return super(LoanAccountsGenerator, self).fetch_batch()
