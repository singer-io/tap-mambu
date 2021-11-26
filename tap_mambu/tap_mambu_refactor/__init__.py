import singer

from .TapGenerators.generator import TapGenerator
from .TapGenerators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from .TapProcessors.processor import TapProcessor


LOGGER = singer.get_logger()


stream_generator_processor_dict = {
    "loan_accounts": ((LoanAccountsADGenerator, LoanAccountsLMGenerator), TapProcessor)
}


def sync_endpoint_refactor(client, catalog, state, stream_name,
                           endpoint_config, sub_type, config):
    generator_classes, processor_class = stream_generator_processor_dict[stream_name]
    generators = [generator_class(stream_name=stream_name,
                                           client=client,
                                           config=config,
                                           state=state,
                                           sub_type=sub_type)
                  for generator_class in generator_classes]
    processor = processor_class(generators=generators,
                                catalog=catalog,
                                stream_name=stream_name)

    processor.process_streams_from_generators()
