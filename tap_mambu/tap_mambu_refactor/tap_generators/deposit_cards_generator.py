from .child_generator import ChildGenerator


class DepositCardsGenerator(ChildGenerator):
    def _init_endpoint_config(self):
        super(DepositCardsGenerator, self)._init_endpoint_config()
        self.endpoint_api_method = "GET"
        self.endpoint_path = f"deposits/{self.endpoint_parent_id}/cards"

    def _init_params(self):
        self.time_extracted = None

    def prepare_batch(self):
        self.params = {}

    def __next__(self):
        if not self.buffer:
            raise StopIteration()
        return self.buffer.pop(0)
