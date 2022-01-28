import singer

from .TapGenerators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from .TapGenerators.loan_repayments_generator import LoanRepaymentsGenerator
from .TapProcessors.loan_accounts_processor import LoanAccountsProcessor
from .TapProcessors.loan_repayments_processor import LoanRepaymentsProcessor

LOGGER = singer.get_logger()

stream_generator_processor_dict = {
    "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor),
    "loan_repayments": ((LoanRepaymentsGenerator,), LoanRepaymentsProcessor)
}


def sync_endpoint_refactor(client, catalog, state,
                           stream_name, sub_type, config, endpoint_config=None):
    generator_classes, processor_class = stream_generator_processor_dict[stream_name]
    generators = [generator_class(stream_name=stream_name,
                                  client=client,
                                  config=config,
                                  state=state,
                                  sub_type=sub_type,
                                  endpoint_config=endpoint_config)
                  for generator_class in generator_classes]
    processor = processor_class(catalog=catalog,
                                stream_name=stream_name)

    return processor.process_streams_from_generators(generators=generators)
