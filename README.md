# tap-mambu

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Mambu 2.0 API](https://api.mambu.com/?shell#Welcome)
- Extracts the following resources:
  - [Branches](https://api.mambu.com/?http#branches-getAll)
  - [Communications](https://api.mambu.com/?http#communications-search)
  - [Centres](https://api.mambu.com/?http#centres-getAll)
  - [Clients](https://api.mambu.com/?http#clients-getAll)
  - [Credit Arrangements](https://api.mambu.com/?http#creditarrangements-getAll)
  - [Custom Field Sets (v1)](https://support.mambu.com/docs/custom-fields-api)
  - [Deposit Accounts](https://api.mambu.com/?http#DepositAccounts-getAll)
    - [Cards](https://api.mambu.com/?http#DepositAccounts-getAllCards)
  - [Deposit Products (v1)](https://support.mambu.com/docs/savings-products-api)
  - [Deposit Transactions](https://api.mambu.com/?http#DepositTransactions-getAll)
  - [Groups](https://api.mambu.com/?http#groups-getAll)
  - [Loan Accounts](https://api.mambu.com/?http#LoanAccounts-getAll)
  - [Loan Products (v1)](https://support.mambu.com/docs/loan-products-api)
  - [Loan Transactions](https://api.mambu.com/?http#LoanTransactions-getAll)
  - [Tasks](https://api.mambu.com/?http#tasks-getAll)
  - [Users](https://api.mambu.com/?http#users-getAll)
  - [GL Accounts](https://support.mambu.com/docs/gl-accounts-api)
  - [GL Journal Entries](https://support.mambu.com/docs/gl-journal-entries-api)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Streams
[**branches (GET v2)**](https://api.mambu.com/?http#branches-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/branches
- Primary keys: id
- Foreign keys: custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**communications (POST v2)**](https://api.mambu.com/?http#communications-search)
- Endpoint: https://instance.sandbox.mambu.com/api/communications/messages:search
- Primary keys: encoded_key
- Foreign keys: group_key (groups), user_key (users), client_key (clients), loan_account_key (loans), deposit_account_key (deposits), sender_key (users), reference_id (from SMS dispatcher)
- Replication strategy: Incremental (query all, filter results)
  - Bookmark query field: creationDate
  - Bookmark: creation_date (date-time)
- Transformations: Fields camelCase to snake_case

[**centres (GET v2)**](https://api.mambu.com/?http#centres-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/centres
- Primary keys: id
- Foreign keys: assigned_branch_key (branches), custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**clients (GET v2)**](https://api.mambu.com/?http#clients-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/clients
- Primary keys: id
- Foreign keys: assigned_user_key (users), assigned_centre_key (centres), custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**credit_arrangements (GET v2)**](https://api.mambu.com/?http#creditarrangements-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/creditarrangements
- Primary keys: id
- Foreign keys: holder_key (?), custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: creationDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**custom_field_sets (GET v1)**](https://support.mambu.com/docs/custom-fields-api)
- Endpoint: https://instance.sandbox.mambu.com/api/customfieldsets
- Primary keys: id
- Foreign keys: None
- Replication strategy: Full table
- Transformations: Fields camelCase to snake_case

[**deposit_accounts (GET v2)**](https://api.mambu.com/?http#DepositAccounts-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/deposits
- Primary keys: id
- Foreign keys: assigned_branch_key (branches), credit_arrangement_key (credit_arrangements), assigned_user_key (users), assigned_centre_key (centres), custom_field_set_id, custom_field_id (custom_field_sets), account_holder_key (?), product_type_key (?)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**cards (GET v2)**](https://api.mambu.com/?http#DepositAccounts-getAllCards)
- Endpoint: https://instance.sandbox.mambu.com/api/deposits/[deposit_id]/cards
- Primary keys: deposit_id, reference_token
- Foreign keys: deposit_id (deposits)
- Replication strategy: Full table (ALL for parent deposit_id)
- Transformations: Fields camelCase to snake_case

[**deposit_products (GET v1)**](https://support.mambu.com/docs/savings-products-api)
- Endpoint: https://instance.sandbox.mambu.com/api/savingsproducts/DSP
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case

[**deposit_transactions (POST v2)**](https://api.mambu.com/?http#DepositTransactions-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/deposits/transactions:search
- Primary keys: encoded_key
- Foreign keys: branch_key (branches), centre_key (centers), user_key (users), linked_loan_transaction_key (loan_transactions), linked_deposit_transaction_key (deposit_transactions)
- Replication strategy: Incremental (query all, filter results)
  - Bookmark query field: creationDate
  - Sort by: creationDate:ASC
  - Bookmark: creation_date (date-time)
- Transformations: Fields camelCase to snake_case

[**groups (GET v2)**](https://api.mambu.com/?http#groups-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/groups
- Primary keys: id
- Foreign keys: client_key (clients), assigned_branch_key (branches), assigned_centre_key (centers), assigned_user_key (users), custom_field_set_id, custom_field_id (custom_field_sets)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**loan_accounts (GET v2)**](https://api.mambu.com/?http#LoanAccounts-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/loans
- Primary keys: id
- Foreign keys: deposit_account_key (deposits), target_deposit_account_key (deposits), assig, ned_user_key (users), assigned_centre_key (centres), assigned_branch_key (branches), credit_arrangement_key (credit_arrangements), custom_field_set_id, custom_field_id (custom_field_sets), account_holder_key (?), product_type_key (?)
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**loan_products (GET v1)**](https://support.mambu.com/docs/loan-products-api)
- Endpoint: https://instance.sandbox.mambu.com/api/loanproducts
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case

[**loan_transactions (POST v2)**](https://api.mambu.com/?http#LoanTransactions-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/loans/transactions:search
- Primary keys: encoded_key
- Foreign keys: branch_key (branches), centre_key (centers), user_key (users), parent_loan_transaction_key (loan_transactions), linked_loan_transaction_key (loan_transactions), linked_deposit_transaction_key (deposit_transactions)
- Replication strategy: Incremental (query all, filter results)
  - Bookmark query field: creationDate
  - Sort by: creationDate:ASC
  - Bookmark: creation_date (date-time)
- Transformations: Fields camelCase to snake_case

[**tasks (GET v2)**](https://api.mambu.com/?http#tasks-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/tasks
- Primary keys: id
- Foreign keys: created_by_user_key (users), assigned_user_key (users), custom_field_set_id, custom_field_id (custom_field_sets) 
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**users (GET v2)**](https://api.mambu.com/?http#users-getAll)
- Endpoint: https://instance.sandbox.mambu.com/api/users
- Primary keys: id
- Foreign keys: branch_key (branches), assigned_branch_key (branches), custom_field_set_id, custom_field_id (custom_field_sets) 
- Replication strategy: Incremental (query all, filter results)
  - Sort by: lastModifiedDate:ASC
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**gl_accounts (GET v1)**](https://support.mambu.com/docs/gl-accounts-api)
- Endpoint: https://instance.sandbox.mambu.com/api/glaccounts
- Primary keys: gl_code
- Replication strategy: Incremental (query filtered based on date and account type)
  - Bookmark: last_modified_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

[**gl_journal_entries (GET v1)**](https://support.mambu.com/docs/gl-journal-entries-api)
- Endpoint: https://instance.sandbox.mambu.com/api/gljournalentries
- Primary keys: entry_id
- Replication strategy: Incremental (query filtered based on date)
  - Bookmark: booking_date (date-time)
- Transformations: Fields camelCase to snake_case, Abstract/generalize custom_field_sets

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-mambu
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `subdomain` is everything before `.mambu.com` in the Mambu instance URL.  For the URL: `https://stitch.sandbox.mambu.com`, the subdomain would be `stitch.sandbox`.

    ```json
    {
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "subdomain": "YOUR_SUBDOMAIN",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-mambu <api_user_email@your_company.com>"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "tasks",
        "bookmarks": {
            "branches": "2019-06-11T13:37:51Z",
            "communications": "2019-06-19T19:48:42Z",
            "centres": "2019-06-18T18:23:53Z",
            "clients": "2019-06-20T00:52:44Z",
            "credit_arrangements": "2019-06-19T19:48:45Z",
            "custom_field_sets": "2019-06-11T13:37:56Z",
            "deposit_accounts": "2019-06-19T19:48:47Z",
            "cards": "2019-06-18T18:23:58Z",
            "deposit_products": "2019-06-20T00:52:49Z",
            "deposit_transactions": "2019-06-20T00:52:40Z",
            "groups": "2019-06-19T19:48:41Z",
            "loan_accounts": "2019-06-11T13:37:52Z",
            "loan_products": "2019-06-20T00:52:43Z",
            "loan_transactions": "2019-06-19T19:48:44Z",
            "tasks": "2019-06-18T18:23:55Z",
            "users": "2019-06-20T00:52:46Z",
            "gl_journal_entries": "2019-11-01T00:00:00.000000Z",
            "gl_accounts": {
                "INCOME": "2019-07-03T15:15:43.000000Z",
                "EQUITY": "2019-07-03T15:15:43.000000Z",
                "LIABILITY": "2019-07-03T15:15:43.000000Z",
                "ASSET": "2019-07-03T15:15:43.000000Z",
                "EXPENSE": "2019-07-03T15:15:43.000000Z"
            }
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-mambu --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-mambu --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-mambu --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-mambu --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the Mambu tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_mambu -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.87/10.
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-mambu --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 265 messages for 16 streams.

        18 schema messages
        203 record messages
        44 state messages

    Details by stream:
    +----------------------+---------+---------+
    | stream               | records | schemas |
    +----------------------+---------+---------+
    | tasks                | 8       | 1       |
    | cards                | 1       | 3       |
    | loan_transactions    | 9       | 1       |
    | deposit_transactions | 31      | 1       |
    | custom_field_sets    | 21      | 1       |
    | groups               | 3       | 1       |
    | communications       | 1       | 1       |
    | branches             | 2       | 1       |
    | credit_arrangements  | 2       | 1       |
    | loan_products        | 2       | 1       |
    | centres              | 2       | 1       |
    | clients              | 104     | 1       |
    | loan_accounts        | 7       | 1       |
    | users                | 3       | 1       |
    | deposit_products     | 4       | 1       |
    | deposit_accounts     | 3       | 1       |
    +----------------------+---------+---------+

    ```
---

Copyright &copy; 2019 Stitch
