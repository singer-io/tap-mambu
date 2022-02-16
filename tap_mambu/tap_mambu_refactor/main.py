import singer

from .tap_generators.deposit_accounts_generator import DepositAccountsGenerator
from .tap_generators.deposit_cards_generator import DepositCardsGenerator
from .tap_generators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from .tap_generators.loan_repayments_generator import LoanRepaymentsGenerator
from .tap_processors.deposit_accounts_processor import DepositAccountsProcessor
from .tap_processors.deposit_cards_processor import DepositCardsProcessor
from .tap_processors.loan_accounts_processor import LoanAccountsProcessor
from .tap_processors.loan_repayments_processor import LoanRepaymentsProcessor

LOGGER = singer.get_logger()


def sync_endpoint_refactor(client, catalog, state,
                           stream_name, sub_type, config, parent_id=None):
    stream_generator_processor_dict = {
        "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor),
        "loan_repayments": ((LoanRepaymentsGenerator,), LoanRepaymentsProcessor),
        "deposit_accounts": ((DepositAccountsGenerator,), DepositAccountsProcessor),
        "cards": ((DepositCardsGenerator,), DepositCardsProcessor)
    }

    generator_classes, processor_class = stream_generator_processor_dict[stream_name]
    generators = [generator_class(stream_name=stream_name,
                                  client=client,
                                  config=config,
                                  state=state,
                                  sub_type=sub_type,
                                  **{} if parent_id is None else {"parent_id": parent_id})
                  for generator_class in generator_classes]
    processor = processor_class(catalog=catalog,
                                stream_name=stream_name,
                                client=client,
                                config=config,
                                state=state,
                                sub_type=sub_type,
                                generators=generators,
                                **{} if parent_id is None else {"parent_id": parent_id})

    return processor.process_streams_from_generators()
