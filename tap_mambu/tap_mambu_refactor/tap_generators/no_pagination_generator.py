from tap_mambu.tap_mambu_refactor.tap_generators.generator import TapGenerator


class NoPaginationGenerator(TapGenerator):
    def _init_params(self):
        self.time_extracted = None

    def prepare_batch(self):
        self.params = {}

    def __next__(self):
        if not self.buffer:
            raise StopIteration()
        return self.buffer.pop(0)
