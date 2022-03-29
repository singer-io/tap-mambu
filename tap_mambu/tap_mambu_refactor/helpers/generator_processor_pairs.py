from ..tap_generators.activities_generator import ActivitiesGenerator
from ..tap_generators.audit_trail_generator import AuditTrailGenerator
from ..tap_generators.branches_generator import BranchesGenerator
from ..tap_generators.centres_generator import CentresGenerator
from ..tap_generators.clients_generator import ClientsGenerator
from ..tap_generators.communications_generator import CommunicationsGenerator
from ..tap_generators.credit_arrangements_generator import CreditArrangementsGenerator
from ..tap_generators.deposit_accounts_generator import DepositAccountsGenerator
from ..tap_generators.deposit_cards_generator import DepositCardsGenerator
from ..tap_generators.installments_generator import InstallmentsGenerator
from ..tap_generators.groups_generator import GroupsGenerator
from ..tap_generators.index_rate_sources_generator import IndexRateSourcesGenerator
from ..tap_generators.interest_accrual_breakdown_generator import InterestAccrualBreakdownGenerator
from ..tap_generators.deposit_transactions_generator import DepositTransactionsGenerator
from ..tap_generators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from ..tap_generators.loan_repayments_generator import LoanRepaymentsGenerator
from ..tap_generators.loan_transactions_generator import LoanTransactionsGenerator
from ..tap_generators.tasks_generator import TasksGenerator
from ..tap_generators.users_generator import UsersGenerator
from ..tap_processors.audit_trail_processor import AuditTrailProcessor
from ..tap_processors.branches_processor import BranchesProcessor
from ..tap_processors.centres_processor import CentresProcessor
from ..tap_processors.clients_processor import ClientsProcessor
from ..tap_processors.communications_processor import CommunicationsProcessor
from ..tap_processors.credit_arrangements_processor import CreditArrangementsProcessor
from ..tap_processors.deposit_accounts_processor import DepositAccountsProcessor
from ..tap_processors.deposit_cards_processor import DepositCardsProcessor
from ..tap_processors.groups_processor import GroupsProcessor
from ..tap_processors.index_rate_sources_processor import IndexRateSourcesProcessor
from ..tap_processors.interest_accrual_breakdown import InterestAccrualBreakdownProcessor
from ..tap_processors.deposit_transactions_processor import DepositTransactionsProcessor
from ..tap_processors.loan_accounts_processor import LoanAccountsProcessor
from ..tap_processors.loan_repayments_processor import LoanRepaymentsProcessor
from ..tap_processors.loan_transactions_processor import LoanTransactionsProcessor
from ..tap_processors.tasks_processor import TasksProcessor
from ..tap_processors.users_processor import UsersProcessor
from ..tap_processors.processor import TapProcessor


def get_generator_processor_pairs():
    return {
        "activities": ((ActivitiesGenerator,), TapProcessor),
        "audit_trail": ((AuditTrailGenerator,), AuditTrailProcessor),
        "branches": ((BranchesGenerator,), BranchesProcessor),
        "cards": ((DepositCardsGenerator,), DepositCardsProcessor),
        "centres": ((CentresGenerator,), CentresProcessor),
        "clients": ((ClientsGenerator,), ClientsProcessor),
        "communications": ((CommunicationsGenerator,), CommunicationsProcessor),
        "credit_arrangements": ((CreditArrangementsGenerator,), CreditArrangementsProcessor),
        "deposit_accounts": ((DepositAccountsGenerator,), DepositAccountsProcessor),
        "groups": ((GroupsGenerator,), GroupsProcessor),
        "index_rate_sources": ((IndexRateSourcesGenerator,), IndexRateSourcesProcessor),
        "deposit_transactions": ((DepositTransactionsGenerator,), DepositTransactionsProcessor),
        "installments": ((InstallmentsGenerator,), TapProcessor),
        "interest_accrual_breakdown": ((InterestAccrualBreakdownGenerator,), InterestAccrualBreakdownProcessor),
        "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor),
        "loan_repayments": ((LoanRepaymentsGenerator,), LoanRepaymentsProcessor),
        "loan_transactions": ((LoanTransactionsGenerator,), LoanTransactionsProcessor),
        "tasks": ((TasksGenerator,), TasksProcessor),
        "users": ((UsersGenerator,), UsersProcessor),
    }


def get_available_streams():
    return list(get_generator_processor_pairs().keys())


def get_generator_processor_for_stream(stream: str):
    stream_generator_processor_dict = get_generator_processor_pairs()
    return stream_generator_processor_dict[stream]
