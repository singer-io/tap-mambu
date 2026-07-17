DEFAULT_PAGE_SIZE = 200
DEFAULT_DATE_WINDOW_SIZE = 1

# Minimal probe config for each stream: method, API path, version, and optional apikey_type.
# Streams with 'parent' are child streams that inherit accessibility from their parent.
# Defaults: version='v2', apikey_type=None.
MINIMAL_POST_BODY = {
    "sortingCriteria": {"field": "encodedKey", "order": "ASC"},
    "filterCriteria": [],
}
PROBE_PARAMS = {"pageSize": 1, "paginationDetails": "OFF", "detailsLevel": "FULL"}
ACTIVITIES_PROBE_PARAMS = {"from": "1970-01-01", "to": "1970-01-01"}

STREAM_PROBE_CONFIG = {
    "activities":                 {
        "method": "GET",
        "path": "activities",
        "version": "v1",
        "params": ACTIVITIES_PROBE_PARAMS,
    },
    "audit_trail":                {"method": "GET",  "path": "v1/events",                            "version": "v1", "apikey_type": "audit"},
    "branches":                   {"method": "GET",  "path": "branches"},
    "cards":                      {"parent": "deposit_accounts"},
    "centres":                    {"method": "GET",  "path": "centres"},
    "clients":                    {"method": "POST", "path": "clients:search"},
    "communications":             {"method": "POST", "path": "communications/messages:search"},
    "credit_arrangements":        {"method": "GET",  "path": "creditarrangements"},
    "custom_field_sets":          {"method": "GET",  "path": "customfieldsets"},
    "deposit_accounts":           {"method": "POST", "path": "deposits:search"},
    "deposit_products":           {"method": "GET",  "path": "depositproducts"},
    "deposit_transactions":       {"method": "POST", "path": "deposits/transactions:search"},
    "gl_accounts":                {"method": "GET",  "path": "glaccounts"},
    "gl_journal_entries":         {"method": "POST", "path": "gljournalentries:search"},
    "groups":                     {"method": "POST", "path": "groups:search"},
    "index_rate_sources":         {"method": "GET",  "path": "indexratesources"},
    "installments":               {"method": "GET",  "path": "installments"},
    "interest_accrual_breakdown": {"method": "POST", "path": "accounting/interestaccrual:search"},
    "loan_accounts":              {"method": "POST", "path": "loans:search"},
    "loan_products":              {"method": "GET",  "path": "loanproducts"},
    "loan_repayments":            {"parent": "loan_accounts"},
    "loan_transactions":          {"method": "POST", "path": "loans/transactions:search"},
    "tasks":                      {"method": "GET",  "path": "tasks"},
    "users":                      {"method": "GET",  "path": "users"},
}
