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
POST_PROBE_PARAMS = {"offset": 0, "limit": 1, "paginationDetails": "OFF", "detailsLevel": "FULL"}
ACTIVITIES_PROBE_PARAMS = {"from": "1970-01-01", "to": "1970-01-01"}
AUDIT_TRAIL_PROBE_PARAMS = {
    "sort_order": "asc",
    "occurred_at[gte]": "1970-01-01T00:00:00Z",
    "occurred_at[lte]": "1970-01-01T00:00:00Z",
}
CLIENTS_PROBE_BODY = {
    "sortingCriteria": {"field": "lastModifiedDate", "order": "ASC"},
    "filterCriteria": [
        {"field": "lastModifiedDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "lastModifiedDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
INSTALLMENTS_PROBE_PARAMS = {
    "dueFrom": "1970-01-01",
    "dueTo": "1970-01-01",
    "detailsLevel": "FULL",
    "paginationDetails": "OFF",
}
COMMUNICATIONS_PROBE_BODY = []

STREAM_PROBE_CONFIG = {
    "activities":                 {
        "method": "GET",
        "path": "activities",
        "version": "v1",
        "params": ACTIVITIES_PROBE_PARAMS,
    },
    "audit_trail":                {
        "method": "GET",
        "path": "v1/events",
        "version": "v1",
        "apikey_type": "audit",
        "params": AUDIT_TRAIL_PROBE_PARAMS,
    },
    "branches":                   {"method": "GET",  "path": "branches"},
    "cards":                      {"parent": "deposit_accounts"},
    "centres":                    {"method": "GET",  "path": "centres"},
    "clients":                    {
        "method": "POST",
        "path": "clients:search",
        "params": POST_PROBE_PARAMS,
        "json": CLIENTS_PROBE_BODY,
    },
    "communications":             {
        "method": "POST",
        "path": "communications/messages:search",
        "params": POST_PROBE_PARAMS,
        "json": COMMUNICATIONS_PROBE_BODY,
    },
    "credit_arrangements":        {"method": "GET",  "path": "creditarrangements"},
    "custom_field_sets":          {"method": "GET",  "path": "customfieldsets"},
    "deposit_accounts":           {"method": "POST", "path": "deposits:search", "params": POST_PROBE_PARAMS},
    "deposit_products":           {"method": "GET",  "path": "depositproducts"},
    "deposit_transactions":       {"method": "POST", "path": "deposits/transactions:search", "params": POST_PROBE_PARAMS},
    "gl_accounts":                {"method": "GET",  "path": "glaccounts"},
    "gl_journal_entries":         {"method": "POST", "path": "gljournalentries:search", "params": POST_PROBE_PARAMS},
    "groups":                     {"method": "POST", "path": "groups:search", "params": POST_PROBE_PARAMS},
    "index_rate_sources":         {"method": "GET",  "path": "indexratesources"},
    "installments":               {
        "method": "GET",
        "path": "installments",
        "params": INSTALLMENTS_PROBE_PARAMS,
    },
    "interest_accrual_breakdown": {"method": "POST", "path": "accounting/interestaccrual:search", "params": POST_PROBE_PARAMS},
    "loan_accounts":              {"method": "POST", "path": "loans:search", "params": POST_PROBE_PARAMS},
    "loan_products":              {"method": "GET",  "path": "loanproducts"},
    "loan_repayments":            {"parent": "loan_accounts"},
    "loan_transactions":          {"method": "POST", "path": "loans/transactions:search", "params": POST_PROBE_PARAMS},
    "tasks":                      {"method": "GET",  "path": "tasks"},
    "users":                      {"method": "GET",  "path": "users"},
}
