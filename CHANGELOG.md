# Changelog

## 2.4.0
  * Misc bug fixes and features [#68](https://github.com/singer-io/tap-mambu/pull/68)
    * added `currency` field to `loan_accounts`
    * added `payment_details` field to `deposit_transactions`
    * added `status` field to `tasks` stream
    * added sorting to `gl_journal_entries` stream to fix missing record at large extractions (very big offset)
    * fixed loan accounts being skipped if updated while the extraction is running
    * replaced `endpoint_config` dict with class properties
    * created unit tests for auxiliary functions
    * added logging to refactored code
    * implemented unit tests and functional tests for `loan_accounts` and `loan_repayments` streams
    * refactored `deposit_accounts` and `cards` streams (including unit tests)

## 2.3.3
  * Enable sorting of `gl_journal_entries` response to prevent missing records due to pagination [#66](https://github.com/singer-io/tap-mambu/pull/66)

## 2.3.2
  * Fix issue with handling the `account_appraisal_date` for `loan_accounts` stream. [#64](https://github.com/singer-io/tap-mambu/pull/64)

## 2.3.1
  * Adjust bookmarking of `loan_accounts` to use `modified_date` *or* `account_appraisal_date` [#62](https://github.com/singer-io/tap-mambu/pull/62)
  * Stream `loan_accounts` and child stream `loan_repayments` refactored into new pattern

## 2.3.0
  * Added `original_account_key` field to the `loan_accounts` stream [#60](https://github.com/singer-io/tap-mambu/pull/60)
  * Fix audit trail duplicated records [#59](https://github.com/singer-io/tap-mambu/pull/59)

## 2.2.0
  * Updates `gl_journal_entries` replication key to `creation_date` to capture entries with reversed transactions [#56](https://github.com/singer-io/tap-mambu/pull/56)

## 2.1.1
  * Query `installments` endpoint correctly using the `start_date` instead of bookmarked value [#54](https://github.com/singer-io/tap-mambu/pull/54)

## 2.1.0
  * Changes `deposit` and `locations` enpoint from GET to POST
  * Adds `Audit trial` stream with new config property `apikey_audit`
  * Adds `parent_account_key` field to `installments` stream [#49](https://github.com/singer-io/tap-mambu/pull/49)

## 2.0.1
  * Fix convert_custom_fields to handle lists as well as dicts [#47](https://github.com/singer-io/tap-mambu/pull/47)

## 2.0.0
  * Update custom field transformations and schema for streams.

## 1.3.3
 * Updates singer-python version to 5.12.1  [#41](https://github.com/singer-io/tap-mambu/pull/41)

## 1.3.2
 * Changed `number` values to use `singer.decimal` in schemas  [#38](https://github.com/singer-io/tap-mambu/pull/38)

## 1.3.0
  * Added integration tests
  * Added api key auth
  * Fixed issue where some streams did not automatically select replication keys

## 1.2.0
  * Added endpoints: activities, index rate sources, installments. Client, Groups, GL Journal Entries endpoints changed.

## 1.1.2
  * Added lookback window for loan transactions stream.

## 1.1.1

 * Fix two fields typed incorrectly in schema reported by bug ticket [#16](https://github.com/singer-io/tap-mambu/pull/16)

## 1.1.0
  * Added a new config parameter `page_size` to allow the limit query param to be customized [#14](https://github.com/singer-io/tap-mambu/pull/14)

## 1.0.5
 * Remove `paginationDetails` parameter from select paged API calls; no longer needed after recent change to Mambu API. This change requested by Mambu to improve Mambu API server performance.

## 1.0.4
 * Fix pagination [#11](https://github.com/singer-io/tap-mambu/pull/11)

## 1.0.3
 * Added json schemas for loan_repayments.

## 1.0.2
 * Fix merge issue with `v.1.0.1`. Revert to `v.1.0.0 `and re-apply pagination changes.

## 1.0.1
 * Change `client.py` and `sync.py` to remove `Items-Total` response header from pagination logic.

## 1.0.0
 * Releasing from Beta --> GA

## 0.0.5
  * Add stream handling for GL Account and GL Journal Entries endpoints

## 0.0.4
  * Update sync.py date handling in process_records and in sync bookmarks for POST endpoints

## 0.0.3
  * Move Transformer initialization "up a level" to remove noise from logging

## 0.0.2
  * Adjust json schemas for loan_products, deposit_products for custom_field_values and other array/lists

## 0.0.1
  * Initial commit
