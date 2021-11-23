import json


class TapProcessor:
    def __init__(self):
        pass

    def process(self, record):
        print(json.dumps(record, indent=2))
