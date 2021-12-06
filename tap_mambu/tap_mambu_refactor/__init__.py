import singer

from .TapGenerators.generator import TapGenerator
from .TapGenerators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from .TapProcessors.loan_accounts_processor import LoanAccountsProcessor
from .TapProcessors.processor import TapProcessor


LOGGER = singer.get_logger()


stream_generator_processor_dict = {
    "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor)
}


def sync_endpoint_refactor(client, catalog, state,
                           stream_name, sub_type, config):
    generator_classes, processor_class = stream_generator_processor_dict[stream_name]
    generators = [generator_class(stream_name=stream_name,
                                           client=client,
                                           config=config,
                                           state=state,
                                           sub_type=sub_type)
                  for generator_class in generator_classes]
    processor = processor_class(catalog=catalog,
                                stream_name=stream_name)

    processor.process_streams_from_generators(generators=generators)
