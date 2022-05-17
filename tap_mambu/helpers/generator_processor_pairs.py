from ..tap_generators.activities_generator import ActivitiesGenerator
from ..tap_generators.audit_trail_generator import AuditTrailGenerator
from ..tap_generators.branches_generator import BranchesGenerator
from ..tap_generators.centres_generator import CentresGenerator
from ..tap_generators.clients_generator import ClientsGenerator
from ..tap_generators.communications_generator import CommunicationsGenerator
from ..tap_generators.credit_arrangements_generator import CreditArrangementsGenerator
from ..tap_generators.custom_field_sets_generator import CustomFieldSetsGenerator
from ..tap_generators.deposit_accounts_generator import DepositAccountsGenerator
from ..tap_generators.deposit_cards_generator import DepositCardsGenerator
from ..tap_generators.deposit_products_generator import DepositProductsGenerator
from ..tap_generators.gl_accounts_generator import GlAccountsGenerator
from ..tap_generators.gl_journal_entries_generator import GlJournalEntriesGenerator
from ..tap_generators.installments_generator import InstallmentsGenerator
from ..tap_generators.groups_generator import GroupsGenerator
from ..tap_generators.index_rate_sources_generator import IndexRateSourcesGenerator
from ..tap_generators.interest_accrual_breakdown_generator import InterestAccrualBreakdownGenerator
from ..tap_generators.deposit_transactions_generator import DepositTransactionsGenerator
from ..tap_generators.loan_accounts_generator import LoanAccountsADGenerator, LoanAccountsLMGenerator
from ..tap_generators.loan_products_generator import LoanProductsGenerator
from ..tap_generators.loan_repayments_generator import LoanRepaymentsGenerator
from ..tap_generators.loan_transactions_generator import LoanTransactionsGenerator
from ..tap_generators.tasks_generator import TasksGenerator
from ..tap_generators.users_generator import UsersGenerator
from ..tap_processors.audit_trail_processor import AuditTrailProcessor
from ..tap_processors.deposit_accounts_processor import DepositAccountsProcessor
from ..tap_processors.deposit_cards_processor import DepositCardsProcessor
from ..tap_processors.loan_accounts_processor import LoanAccountsProcessor
from ..tap_processors.loan_repayments_processor import LoanRepaymentsProcessor
from ..tap_processors.processor import TapProcessor


def get_generator_processor_pairs():
    return {
        "activities": ((ActivitiesGenerator,), TapProcessor),
        "audit_trail": ((AuditTrailGenerator,), AuditTrailProcessor),
        "branches": ((BranchesGenerator,), TapProcessor),
        "cards": ((DepositCardsGenerator,), DepositCardsProcessor),
        "centres": ((CentresGenerator,), TapProcessor),
        "clients": ((ClientsGenerator,), TapProcessor),
        "communications": ((CommunicationsGenerator,), TapProcessor),
        "credit_arrangements": ((CreditArrangementsGenerator,), TapProcessor),
        "custom_field_sets": ((CustomFieldSetsGenerator,), TapProcessor),
        "deposit_accounts": ((DepositAccountsGenerator,), DepositAccountsProcessor),
        "deposit_products": ((DepositProductsGenerator,), TapProcessor),
        "gl_accounts": ((GlAccountsGenerator,), TapProcessor),
        "gl_journal_entries": ((GlJournalEntriesGenerator,), TapProcessor),
        "groups": ((GroupsGenerator,), TapProcessor),
        "index_rate_sources": ((IndexRateSourcesGenerator,), TapProcessor),
        "deposit_transactions": ((DepositTransactionsGenerator,), TapProcessor),
        "installments": ((InstallmentsGenerator,), TapProcessor),
        "interest_accrual_breakdown": ((InterestAccrualBreakdownGenerator,), TapProcessor),
        "loan_accounts": ((LoanAccountsLMGenerator, LoanAccountsADGenerator), LoanAccountsProcessor),
        "loan_products": ((LoanProductsGenerator,), TapProcessor),
        "loan_repayments": ((LoanRepaymentsGenerator,), LoanRepaymentsProcessor),
        "loan_transactions": ((LoanTransactionsGenerator,), TapProcessor),
        "tasks": ((TasksGenerator,), TapProcessor),
        "users": ((UsersGenerator,), TapProcessor),
    }


def get_available_streams():
    return list(get_generator_processor_pairs().keys())


def get_generator_processor_for_stream(stream: str):
    stream_generator_processor_dict = get_generator_processor_pairs()
    return stream_generator_processor_dict[stream]


def get_stream_subtypes(stream: str):
    if stream in ["gl_accounts"]:
        return ["ASSET", "LIABILITY", "EQUITY", "INCOME", "EXPENSE"]
    return ["self"]
