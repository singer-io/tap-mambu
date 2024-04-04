from .generator import TapGenerator
from typing import List


class ChildGenerator(TapGenerator):
    def __init__(self, stream_name, client, config, state, sub_type, parent_id):
        self.endpoint_parent_id = parent_id
        super(ChildGenerator, self).__init__(stream_name, client, config, state, sub_type)

    def _init_endpoint_config(self):
        super(ChildGenerator, self)._init_endpoint_config()
        self.endpoint_path = f"{self.endpoint_parent_id}"  # include parent id in endpoint path

    def _init_buffers(self):
        self.buffer: List = list()
        self.max_buffer_size = 1000

