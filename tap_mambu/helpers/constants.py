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
DEPOSIT_ACCOUNTS_PROBE_BODY = {
    "sortingCriteria": {"field": "lastModifiedDate", "order": "ASC"},
    "filterCriteria": [
        {"field": "lastModifiedDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "lastModifiedDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
DEPOSIT_TRANSACTIONS_PROBE_BODY = {
    "sortingCriteria": {"field": "id", "order": "ASC"},
    "filterCriteria": [
        {"field": "creationDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "creationDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
GL_ACCOUNTS_PROBE_PARAMS = {
    "offset": 0,
    "limit": 1,
    "paginationDetails": "OFF",
    "detailsLevel": "FULL",
    "type": "ASSET",
}
GL_JOURNAL_ENTRIES_PROBE_BODY = {
    "sortingCriteria": {"field": "entryId", "order": "ASC"},
    "filterCriteria": [
        {"field": "creationDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "creationDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
GROUPS_PROBE_BODY = {
    "sortingCriteria": {"field": "lastModifiedDate", "order": "ASC"},
    "filterCriteria": [
        {"field": "lastModifiedDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "lastModifiedDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
INTEREST_ACCRUAL_BREAKDOWN_PROBE_BODY = {
    "sortingCriteria": {"field": "entryId", "order": "ASC"},
    "filterCriteria": [
        {"field": "creationDate", "operator": "AFTER", "value": "1970-01-01"},
        {"field": "creationDate", "operator": "BEFORE", "value": "1970-01-02"},
    ],
}
LOAN_ACCOUNTS_PROBE_BODY = {
    "sortingCriteria": {"field": "id", "order": "ASC"},
    "filterCriteria": [
        {"field": "lastModifiedDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "lastModifiedDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
LOAN_TRANSACTIONS_PROBE_BODY = {
    "sortingCriteria": {"field": "id", "order": "ASC"},
    "filterCriteria": [
        {"field": "creationDate", "operator": "AFTER", "value": "1970-01-01T00:00:00Z"},
        {"field": "creationDate", "operator": "BEFORE", "value": "1970-01-02T00:00:00Z"},
    ],
}
NO_PAGINATION_PROBE_PARAMS = {}
INSTALLMENTS_PROBE_PARAMS = {
    "dueFrom": "1970-01-01",
    "dueTo": "1970-01-01",
    "detailsLevel": "FULL",
    "paginationDetails": "OFF",
}
SORT_BY_CREATION_DATE_PROBE_PARAMS = {
    "pageSize": 1,
    "paginationDetails": "OFF",
    "detailsLevel": "FULL",
    "sortBy": "creationDate:ASC",
}
SORT_BY_ID_PROBE_PARAMS = {
    "pageSize": 1,
    "paginationDetails": "OFF",
    "detailsLevel": "FULL",
    "sortBy": "id:ASC",
}
SORT_BY_LAST_MODIFIED_DATE_PROBE_PARAMS = {
    "pageSize": 1,
    "paginationDetails": "OFF",
    "detailsLevel": "FULL",
    "sortBy": "lastModifiedDate:ASC",
}
COMMUNICATIONS_PROBE_BODY = [
    {
        "field": "creationDate",
        "operator": "AFTER",
        "value": "1970-01-01T00:00:00Z",
    },
    {
        "field": "creationDate",
        "operator": "BEFORE",
        "value": "1970-01-02T00:00:00Z",
    },
    {
        "field": "state",
        "operator": "EQUALS",
        "value": "SENT",
    },
]

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
    "credit_arrangements":        {
        "method": "GET",
        "path": "creditarrangements",
        "params": SORT_BY_CREATION_DATE_PROBE_PARAMS,
    },
    "custom_field_sets":          {"method": "GET",  "path": "customfieldsets"},
    "deposit_accounts":           {
        "method": "POST",
        "path": "deposits:search",
        "params": POST_PROBE_PARAMS,
        "json": DEPOSIT_ACCOUNTS_PROBE_BODY,
    },
    "deposit_products":           {
        "method": "GET",
        "path": "depositproducts",
        "params": POST_PROBE_PARAMS,
    },
    "deposit_transactions":       {
        "method": "POST",
        "path": "deposits/transactions:search",
        "params": POST_PROBE_PARAMS,
        "json": DEPOSIT_TRANSACTIONS_PROBE_BODY,
    },
    "gl_accounts":                {
        "method": "GET",
        "path": "glaccounts",
        "params": GL_ACCOUNTS_PROBE_PARAMS,
    },
    "gl_journal_entries":         {
        "method": "POST",
        "path": "gljournalentries:search",
        "params": POST_PROBE_PARAMS,
        "json": GL_JOURNAL_ENTRIES_PROBE_BODY,
    },
    "groups":                     {
        "method": "POST",
        "path": "groups:search",
        "params": POST_PROBE_PARAMS,
        "json": GROUPS_PROBE_BODY,
    },
    "index_rate_sources":         {
        "method": "GET",
        "path": "indexratesources",
        "params": NO_PAGINATION_PROBE_PARAMS,
    },
    "installments":               {
        "method": "GET",
        "path": "installments",
        "params": INSTALLMENTS_PROBE_PARAMS,
    },
    "interest_accrual_breakdown": {
        "method": "POST",
        "path": "accounting/interestaccrual:search",
        "params": POST_PROBE_PARAMS,
        "json": INTEREST_ACCRUAL_BREAKDOWN_PROBE_BODY,
    },
    "loan_accounts":              {
        "method": "POST",
        "path": "loans:search",
        "params": POST_PROBE_PARAMS,
        "json": LOAN_ACCOUNTS_PROBE_BODY,
    },
    "loan_products":              {
        "method": "GET",
        "path": "loanproducts",
        "params": SORT_BY_ID_PROBE_PARAMS,
    },
    "loan_repayments":            {"parent": "loan_accounts"},
    "loan_transactions":          {
        "method": "POST",
        "path": "loans/transactions:search",
        "params": POST_PROBE_PARAMS,
        "json": LOAN_TRANSACTIONS_PROBE_BODY,
    },
    "tasks":                      {
        "method": "GET",
        "path": "tasks",
        "params": SORT_BY_LAST_MODIFIED_DATE_PROBE_PARAMS,
    },
    "users":                      {
        "method": "GET",
        "path": "users",
        "params": SORT_BY_ID_PROBE_PARAMS,
    },
}
