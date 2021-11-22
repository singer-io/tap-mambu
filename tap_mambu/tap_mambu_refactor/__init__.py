from tap_mambu.tap_mambu_refactor.TapGenerators.generator import TapGenerator
from tap_mambu.tap_mambu_refactor.TapProcessors.processor import TapProcessor

stream_generator_processor_dict = {
    "loan_accounts": (TapGenerator, TapProcessor)
}


def sync_endpoint_refactor(client, catalog, state, start_date, stream_name,
                           path, endpoint_config, sub_type):
    generator_class, processor_class = stream_generator_processor_dict[stream_name]
    generator, processor = generator_class(client), processor_class()
    for record in generator:
        processor.process(record)
