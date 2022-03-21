import singer

from .tap_generators.branches_generator import BranchesGenerator
from .tap_generators.centres_generator import CentresGenerator
from .tap_generators.clients_generator import ClientsGenerator
from .tap_generators.communications_generator import CommunicationsGenerator
from .tap_generators.deposit_accounts_generator import DepositAccountsGenerator
from .tap_generators.deposit_cards_generator import DepositCardsGenerator
from .tap_generators.installments_generator import InstallmentsGenerator
from .tap_generators.groups_generator import GroupsGenerator
from .tap_generators.index_rate_sources_generator import IndexRateSourcesGenerator
from .tap_generators.deposit_transactions_generator import DepositTransactionsGenerator
from .tap_generators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from .tap_generators.loan_repayments_generator import LoanRepaymentsGenerator
from .tap_generators.loan_transactions_generator import LoanTransactionsGenerator
from .tap_generators.tasks_generator import TasksGenerator
from .tap_processors.branches_processor import BranchesProcessor
from .tap_processors.centres_processor import CentresProcessor
from .tap_processors.clients_processor import ClientsProcessor
from .tap_processors.communications_processor import CommunicationsProcessor
from .tap_processors.deposit_accounts_processor import DepositAccountsProcessor
from .tap_processors.deposit_cards_processor import DepositCardsProcessor
from .tap_processors.groups_processor import GroupsProcessor
from .tap_processors.index_rate_sources_processor import IndexRateSourcesProcessor
from .tap_processors.deposit_transactions_processor import DepositTransactionsProcessor
from .tap_processors.loan_accounts_processor import LoanAccountsProcessor
from .tap_processors.loan_repayments_processor import LoanRepaymentsProcessor
from .tap_processors.loan_transactions_processor import LoanTransactionsProcessor
from .tap_processors.tasks_processor import TasksProcessor
from .tap_processors.processor import TapProcessor

LOGGER = singer.get_logger()


def sync_endpoint_refactor(client, catalog, state,
                           stream_name, sub_type, config, parent_id=None):
    stream_generator_processor_dict = {
        "branches": ((BranchesGenerator,), BranchesProcessor),
        "cards": ((DepositCardsGenerator,), DepositCardsProcessor),
        "centres": ((CentresGenerator,), CentresProcessor),
        "clients": ((ClientsGenerator,), ClientsProcessor),
        "communications": ((CommunicationsGenerator,), CommunicationsProcessor),
        "deposit_accounts": ((DepositAccountsGenerator,), DepositAccountsProcessor),
        "groups": ((GroupsGenerator,), GroupsProcessor),
        "index_rate_sources": ((IndexRateSourcesGenerator,), IndexRateSourcesProcessor),
        "deposit_transactions": ((DepositTransactionsGenerator,), DepositTransactionsProcessor),
        "installments": ((InstallmentsGenerator,), TapProcessor),
        "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor),
        "loan_repayments": ((LoanRepaymentsGenerator,), LoanRepaymentsProcessor),
        "loan_transactions": ((LoanTransactionsGenerator,), LoanTransactionsProcessor),
        "tasks": ((TasksGenerator,), TasksProcessor),
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
