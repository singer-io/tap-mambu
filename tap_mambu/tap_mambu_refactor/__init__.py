import singer

from tap_mambu.tap_mambu_refactor.TapGenerators.generator import TapGenerator
from tap_mambu.tap_mambu_refactor.TapGenerators.loan_accounts_generator import LoanAccountsGenerator
from tap_mambu.tap_mambu_refactor.TapProcessors.processor import TapProcessor


LOGGER = singer.get_logger()


stream_generator_processor_dict = {
    "loan_accounts": (LoanAccountsGenerator, TapProcessor)
}


def sync_endpoint_refactor(client, catalog, state, stream_name,
                           endpoint_config, sub_type, config):
    generator_class, processor_class = stream_generator_processor_dict[stream_name]
    generator = generator_class(stream_name=stream_name,
                                           client=client,
                                           endpoint_config=endpoint_config,
                                           config=config,
                                           state=state,
                                           sub_type=sub_type)
    processor = processor_class(generator=generator,
                                catalog=catalog,
                                stream_name=stream_name,
                                endpoint_config=endpoint_config)

    processor.process_stream_from_generator()
