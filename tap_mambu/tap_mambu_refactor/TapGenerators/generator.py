import time
from typing import List

import requests


class TapGenerator:
    def __init__(self, client):
        self.buffer: List = list()
        self.start_date = 0

    def __iter__(self):
        self.buffer = self.fetch_batch()
        return self

    def __next__(self):
        if not self.buffer:
            self.buffer = self.fetch_batch()
        if not self.buffer:
            raise StopIteration()
        return self.buffer.pop(0)

    def fetch_batch(self):
        if not self.buffer:
            # fetch lm records
            pass
        return list()
