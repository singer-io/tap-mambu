from gettext import install
import sys
from urllib import response
from .child_generator import ChildGenerator
from typing import List
from singer import utils, get_logger

from ..helpers import transform_json
from ..helpers.hashable_dict import HashableDict

LOGGER = get_logger()


class LoanRepaymentsGenerator(ChildGenerator):

    def _init_endpoint_config(self):
        super(LoanRepaymentsGenerator, self)._init_endpoint_config()
        self.endpoint_api_version = "v2"
        self.endpoint_api_method = "GET"
        # include parent id in endpoint path
        self.endpoint_path = f"loans/{self.endpoint_parent_id}/schedule"
    
    # Override the Generator method to retrieve schedule information from mambu apis
    def fetch_batch(self):
        endpoint_querystring = '&'.join([f'{key}={value}' for (key, value) in self.params.items()])

        LOGGER.info(f'(generator) Stream {self.stream_name} - URL for {self.stream_name} ({self.endpoint_api_method}, '
                    f'{self.endpoint_api_version}): {self.client.base_url}/{self.endpoint_path}?{endpoint_querystring}')
        LOGGER.info(f'(generator) Stream {self.stream_name} - body = {self.endpoint_body}')

        response = self.client.request(
            method=self.endpoint_api_method,
            path=self.endpoint_path,
            version=self.endpoint_api_version,
            apikey_type=self.endpoint_api_key_type,
            params=endpoint_querystring,
            endpoint=self.stream_name,
            json=self.endpoint_body
        )

        response = self._format_response(response)

        self.time_extracted = utils.now()
        LOGGER.info(f'(generator) Stream {self.stream_name} - extracted records: {len(response)}')
        return self.transform_batch(response)
    
    def _format_response(self, response):  
        installments = []
        for installment in response['installments']:
            installment.update(response['currency'])
            installments.append(installment)
        
        return installments
        
        
