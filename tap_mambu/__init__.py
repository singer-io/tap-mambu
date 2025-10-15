#!/usr/bin/env python3

import sys
import json
import singer

from tap_mambu.helpers.constants import DEFAULT_PAGE_SIZE
from tap_mambu.helpers.client import MambuClient
from tap_mambu.helpers.discover import discover
from tap_mambu.sync import sync_all_streams

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'subdomain',
    'start_date',
    'user_agent'
]


def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with MambuClient(parsed_args.config.get('username'),
                     parsed_args.config.get('password'),
                     parsed_args.config.get('apikey'),
                     parsed_args.config['subdomain'],
                     parsed_args.config.get('apikey_audit'),
                     int(parsed_args.config.get('page_size', DEFAULT_PAGE_SIZE)),
                     user_agent=parsed_args.config['user_agent'],
                     window_size=parsed_args.config.get('window_size')) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync_all_streams(client=client,
                             config=parsed_args.config,
                             catalog=parsed_args.catalog,
                             state=state)


if __name__ == '__main__':
    main()
