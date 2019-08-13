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
  - [Custom Field Sets](https://api.mambu.com/?http#customfieldsets-getAll)
  - [Deposits](https://api.mambu.com/?http#DepositAccounts-getAll)
    - [Cards](https://api.mambu.com/?http#DepositAccounts-getAllCards)
  - [Deposit Transactions](https://api.mambu.com/?http#DepositTransactions-getAll)
  - [Groups](https://api.mambu.com/?http#groups-getAll)
  - [Loans](https://api.mambu.com/?http#LoanAccounts-getAll)
  - [Loan Transactions](https://api.mambu.com/?http#LoanTransactions-getAll)
  - [Tasks](https://api.mambu.com/?http#tasks-getAll)
  - [Users](https://api.mambu.com/?http#users-getAll)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

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
3. Create your tap's `config.json` file which should look like the following:

    ```json
    {
        "username": "YOUR_ACCESS_TOKEN",
        "password": "YOUR_PASSWORD",
        "base_url": "https://YOUR_INSTANCE<.sandbox>.mambu.com",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-mambu <api_user_email@your_company.com>"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "tasks",
        "bookmarks": {
            "branches": "2019-06-11T13:37:55Z",
            "communications": "2019-06-19T19:48:42Z",
            "centres": "2019-06-18T18:23:58Z",
            "clients": "2019-06-20T00:52:46Z",
            "credit_arrangements": "2019-06-19T19:48:44Z",
            "custom_field_sets": "2019-06-11T13:37:55Z",
            "deposits": "2019-06-19T19:48:42Z",
            "cards": "2019-06-18T18:23:58Z",
            "deposit_transactions": "2019-06-20T00:52:46Z",
            "groups": "2019-06-19T19:48:44Z",
            "loans": "2019-06-11T13:37:55Z",
            "loan_transactions": "2019-06-19T19:48:42Z",
            "tasks": "2019-06-18T18:23:58Z",
            "users": "2019-06-20T00:52:46Z"
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
    Your code has been rated at TBD
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-mambu --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained TBD.
    ```
---

Copyright &copy; 2019 Stitch
